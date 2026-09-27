// Copyright (c) 2025 The TQUIC Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! tquic_mpsim runs a TQUIC client and a TQUIC server in one process and
//! connects them through emulated network paths. Each path has a bottleneck
//! bandwidth, a one-way delay, a drop-tail queue, random or bursty
//! (Gilbert-Elliott) loss and optional outage windows.
//!
//! A live video source writes frames to the connection at a fixed frame rate
//! and the receiver records the time at which each frame becomes readable.
//! The summary reports frame latency, the ratio of frames missing a playout
//! deadline, a rebuffering model and per-path transport statistics, so the
//! multipath schedulers can be compared for live streaming under loss.
//!
//! The emulation runs in real time because TQUIC reads the system clock
//! internally.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde_json::json;
use serde_json::Value;

use tquic::Config;
use tquic::CongestionControlAlgorithm;
use tquic::Connection;
use tquic::Endpoint;
use tquic::MultipathAlgorithm;
use tquic::PacketInfo;
use tquic::PacketSendHandler;
use tquic::TlsConfig;
use tquic::TransportHandler;

/// Largest UDP payload carried by an emulated path (1500 byte MTU).
const MAX_UDP_PAYLOAD: usize = 1472;

/// IPv4 and UDP header bytes counted against the path bandwidth.
const IP_UDP_OVERHEAD: usize = 28;

/// Frame header: sequence (u32), generation time in us (u64), payload length (u32).
const FRAME_HDR_LEN: usize = 16;

/// Width of the buckets in the per-path timeline.
const TIMELINE_BUCKET: Duration = Duration::from_millis(100);

/// Playout deadlines and startup buffers (ms) used by the player models.
const DEADLINES_MS: [u64; 6] = [100, 200, 300, 500, 1000, 2000];

/// Sampling interval of the sender's congestion state.
const CC_SAMPLE_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Parser, Debug, Clone)]
#[clap(name = "tquic_mpsim", version = env!("CARGO_PKG_VERSION"))]
struct SimOpt {
    /// Scheduling mode: single (multipath disabled, first path only), minrtt,
    /// redundant or roundrobin.
    #[clap(long, default_value = "minrtt")]
    mode: String,

    /// Path specification, repeat once per path. Comma separated key=value
    /// pairs: bw=<Mbit/s>, delay=<one-way ms>, loss=<percent>,
    /// burst=<mean loss burst length in packets>,
    /// burstms=<mean loss burst duration in ms>, queue=<max queuing delay ms>,
    /// outage=<start s>-<end s> (repeatable, relative to the stream start,
    /// `inf` allowed as end).
    #[clap(long = "path", required = true, value_parser = parse_path_spec)]
    paths: Vec<PathSpec>,

    /// Direction of the live stream: down (server to client) or up (client
    /// to server).
    #[clap(long, default_value = "down")]
    direction: String,

    /// Stream mapping: single (one stream for the whole session) or frame
    /// (one unidirectional stream per video frame).
    #[clap(long, default_value = "single")]
    stream_mode: String,

    /// Video bitrate in Mbit/s.
    #[clap(long, default_value_t = 4.0)]
    bitrate: f64,

    /// Video frame rate.
    #[clap(long, default_value_t = 30)]
    fps: u32,

    /// GOP length in frames.
    #[clap(long, default_value_t = 60)]
    gop: u32,

    /// Size ratio between an I-frame and a P-frame.
    #[clap(long, default_value_t = 5.0)]
    iframe_ratio: f64,

    /// Video duration in seconds.
    #[clap(long, default_value_t = 30.0)]
    duration: f64,

    /// Seconds between connection establishment and the stream start.
    #[clap(long, default_value_t = 1.0)]
    warmup: f64,

    /// Seconds to keep running after the last frame so late frames can arrive.
    #[clap(long, default_value_t = 5.0)]
    drain: f64,

    /// Congestion control algorithm: bbr, bbr3, cubic, copa or dummy.
    #[clap(long, default_value = "bbr")]
    cc: String,

    /// Upper limit of the probe timeout in ms (0 keeps the library default).
    #[clap(long, default_value_t = 0)]
    max_pto: u64,

    /// Idle timeout in ms.
    #[clap(long, default_value_t = 30000)]
    idle_timeout: u64,

    /// Seed for loss processes and frame sizes.
    #[clap(long, default_value_t = 1)]
    seed: u64,

    /// Free form label copied to the summary.
    #[clap(long, default_value = "")]
    label: String,

    /// Write the JSON summary to this file instead of stdout.
    #[clap(long)]
    out: Option<String>,

    /// Write per-frame records as CSV to this file.
    #[clap(long)]
    frames_csv: Option<String>,

    /// Write the per-path traffic timeline as CSV to this file.
    #[clap(long)]
    timeline_csv: Option<String>,

    /// Write the sender's per-path congestion state, sampled every 20 ms, as
    /// CSV to this file.
    #[clap(long)]
    cc_csv: Option<String>,

    /// Log level of the TQUIC library: off, error, warn, info, debug, trace.
    #[clap(long, default_value = "off")]
    log_level: log::LevelFilter,

    /// Enable trace logging for 20 ms when the event loop starts spinning
    /// (more than 20000 iterations in 100 ms), to see what keeps it busy.
    #[clap(long)]
    trace_on_spin: bool,

    /// Server certificate file.
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/../src/tls/testdata/cert.crt"))]
    cert_file: String,

    /// Server private key file.
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/../src/tls/testdata/cert.key"))]
    key_file: String,
}

/// Parameters of an emulated path. Both directions use the same parameters
/// but independent loss processes.
#[derive(Clone, Debug)]
struct PathSpec {
    bw_mbps: f64,
    delay_ms: f64,
    loss: f64,
    burst: f64,
    burst_ms: f64,
    queue_ms: f64,
    outages: Vec<(f64, f64)>,
}

fn parse_path_spec(s: &str) -> Result<PathSpec, String> {
    PathSpec::from_str(s)
}

impl FromStr for PathSpec {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let mut spec = PathSpec {
            bw_mbps: 10.0,
            delay_ms: 20.0,
            loss: 0.0,
            burst: 1.0,
            burst_ms: 0.0,
            queue_ms: 100.0,
            outages: Vec::new(),
        };
        for kv in s.split(',').filter(|kv| !kv.is_empty()) {
            let (k, v) = kv
                .split_once('=')
                .ok_or(format!("invalid path option {}", kv))?;
            let num = |v: &str| v.parse::<f64>().map_err(|e| format!("{}: {}", k, e));
            match k {
                "bw" => spec.bw_mbps = num(v)?,
                "delay" => spec.delay_ms = num(v)?,
                "loss" => spec.loss = num(v)? / 100.0,
                "burst" => spec.burst = num(v)?,
                "burstms" => spec.burst_ms = num(v)?,
                "queue" => spec.queue_ms = num(v)?,
                "outage" => {
                    let (a, b) = v.split_once('-').ok_or(format!("invalid outage {}", v))?;
                    spec.outages.push((num(a)?, num(b)?));
                }
                _ => return Err(format!("unknown path option {}", k)),
            }
        }
        if spec.bw_mbps <= 0.0 || !(0.0..1.0).contains(&spec.loss) {
            return Err(format!("invalid path {}", s));
        }
        Ok(spec)
    }
}

impl PathSpec {
    fn to_json(&self) -> Value {
        json!({
            "bw_mbps": self.bw_mbps,
            "delay_ms": self.delay_ms,
            "loss": self.loss,
            "burst": self.burst,
            "burst_ms": self.burst_ms,
            "queue_ms": self.queue_ms,
            "outages": self.outages.iter().map(|(a, b)| {
                json!([a, if b.is_finite() { json!(b) } else { Value::Null }])
            }).collect::<Vec<_>>(),
        })
    }
}

#[derive(Default, Clone, Debug)]
struct LinkStats {
    pkts_in: u64,
    bytes_in: u64,
    pkts_out: u64,
    bytes_out: u64,
    drop_loss: u64,
    drop_queue: u64,
    drop_outage: u64,
    drop_mtu: u64,
}

/// One direction of an emulated path: a drop-tail bottleneck queue followed
/// by a lossy link with a fixed propagation delay.
struct Link {
    spec: PathSpec,
    busy_until: Option<Instant>,
    in_bad_state: bool,
    /// State of the time-based loss process and when it ends.
    timed_state: Option<(bool, Instant)>,
    rng: StdRng,
    stats: LinkStats,
    /// Offered and delivered bytes per timeline bucket.
    timeline: Vec<(u64, u64)>,
}

impl Link {
    fn new(spec: PathSpec, seed: u64) -> Self {
        Link {
            spec,
            busy_until: None,
            in_bad_state: false,
            timed_state: None,
            rng: StdRng::seed_from_u64(seed),
            stats: LinkStats::default(),
            timeline: Vec::new(),
        }
    }

    /// Decide whether a packet sent at `now` is lost.
    ///
    /// With `burst_ms` set, the link alternates between lossless good periods
    /// and bad periods that drop every packet. Both durations are exponential;
    /// the mean bad period is `burst_ms` and the share of time in the bad
    /// state equals `spec.loss`.
    ///
    /// With `burst` above one, a per-packet Gilbert-Elliott model (as in
    /// netem) is used instead. Its bursts are counted in packets, so they last
    /// longer in time when traffic is sparse.
    fn lose(&mut self, now: Instant) -> bool {
        let p = self.spec.loss;
        if p <= 0.0 {
            return false;
        }
        if self.spec.burst_ms > 0.0 {
            let mean_bad = self.spec.burst_ms / 1000.0;
            let mean_good = mean_bad * (1.0 - p) / p;
            loop {
                let (bad, until) = match self.timed_state {
                    Some(s) => s,
                    None => {
                        let d = -mean_good * (1.0 - self.rng.gen::<f64>()).ln();
                        let s = (false, now + Duration::from_secs_f64(d));
                        self.timed_state = Some(s);
                        s
                    }
                };
                if now < until {
                    return bad;
                }
                let mean = if bad { mean_good } else { mean_bad };
                let d = -mean * (1.0 - self.rng.gen::<f64>()).ln();
                self.timed_state = Some((!bad, until + Duration::from_secs_f64(d)));
            }
        }
        if self.spec.burst <= 1.0 {
            return self.rng.gen::<f64>() < p;
        }
        let p_bad_to_good = 1.0 / self.spec.burst;
        let p_good_to_bad = p * p_bad_to_good / (1.0 - p);
        let r = self.rng.gen::<f64>();
        if self.in_bad_state {
            if r < p_bad_to_good {
                self.in_bad_state = false;
            }
        } else if r < p_good_to_bad {
            self.in_bad_state = true;
        }
        self.in_bad_state
    }

    fn record(&mut self, now: Instant, base: Instant, offered: usize, delivered: usize) {
        let idx =
            (now.saturating_duration_since(base).as_nanos() / TIMELINE_BUCKET.as_nanos()) as usize;
        if self.timeline.len() <= idx {
            self.timeline.resize(idx + 1, (0, 0));
        }
        self.timeline[idx].0 += offered as u64;
        self.timeline[idx].1 += delivered as u64;
    }

    /// Enqueue a packet and return its arrival time, or None if it is dropped.
    fn enqueue(
        &mut self,
        now: Instant,
        size: usize,
        base: Instant,
        stream_start: Option<Instant>,
    ) -> Option<Instant> {
        self.stats.pkts_in += 1;
        self.stats.bytes_in += size as u64;

        if size > MAX_UDP_PAYLOAD {
            self.stats.drop_mtu += 1;
            self.record(now, base, size, 0);
            return None;
        }

        if let Some(start) = stream_start {
            let t = now.saturating_duration_since(start).as_secs_f64();
            if now >= start && self.spec.outages.iter().any(|&(a, b)| t >= a && t < b) {
                self.stats.drop_outage += 1;
                self.record(now, base, size, 0);
                return None;
            }
        }

        let tx_start = match self.busy_until {
            Some(t) if t > now => t,
            _ => now,
        };
        if tx_start - now > Duration::from_secs_f64(self.spec.queue_ms / 1000.0) {
            self.stats.drop_queue += 1;
            self.record(now, base, size, 0);
            return None;
        }
        let tx_time = Duration::from_secs_f64(
            (size + IP_UDP_OVERHEAD) as f64 * 8.0 / (self.spec.bw_mbps * 1e6),
        );
        let tx_end = tx_start + tx_time;
        self.busy_until = Some(tx_end);

        // A corrupted packet still occupies the link.
        if self.lose(now) {
            self.stats.drop_loss += 1;
            self.record(now, base, size, 0);
            return None;
        }

        self.stats.pkts_out += 1;
        self.stats.bytes_out += size as u64;
        self.record(now, base, size, size);
        Some(tx_end + Duration::from_secs_f64(self.spec.delay_ms / 1000.0))
    }

    fn stats_json(&self) -> Value {
        let s = &self.stats;
        json!({
            "pkts_in": s.pkts_in,
            "bytes_in": s.bytes_in,
            "pkts_out": s.pkts_out,
            "bytes_out": s.bytes_out,
            "drop_loss": s.drop_loss,
            "drop_queue": s.drop_queue,
            "drop_outage": s.drop_outage,
            "drop_mtu": s.drop_mtu,
        })
    }
}

/// A packet travelling on an emulated path.
struct Arrival {
    at: Instant,
    seq: u64,
    to_server: bool,
    data: Vec<u8>,
    src: SocketAddr,
    dst: SocketAddr,
}

impl PartialEq for Arrival {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}

impl Eq for Arrival {}

impl PartialOrd for Arrival {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Arrival {
    // Reversed so that BinaryHeap pops the earliest arrival first.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

/// Packets an endpoint has handed over for sending.
type OutgoingPackets = Rc<RefCell<Vec<(Vec<u8>, PacketInfo)>>>;

/// Collects the packets an endpoint wants to send.
struct PacketCollector {
    out: OutgoingPackets,
}

impl PacketSendHandler for PacketCollector {
    fn on_packets_send(&self, pkts: &[(Vec<u8>, PacketInfo)]) -> tquic::Result<usize> {
        self.out.borrow_mut().extend_from_slice(pkts);
        Ok(pkts.len())
    }
}

/// Application state of one endpoint.
struct Side {
    is_client: bool,
    extra_paths: Vec<(SocketAddr, SocketAddr)>,
    conn_idx: Option<u64>,
    established_at: Option<Instant>,
    multipath: bool,
    closed: bool,
    add_path_errors: Vec<String>,

    /// Partially received frames per stream.
    rx_bufs: HashMap<u64, Vec<u8>>,
    /// Sequence number and completion time of received frames.
    rx_frames: Vec<(u32, Instant)>,
    rx_bytes: u64,
}

impl Side {
    fn new(is_client: bool, extra_paths: Vec<(SocketAddr, SocketAddr)>) -> Self {
        Side {
            is_client,
            extra_paths,
            conn_idx: None,
            established_at: None,
            multipath: false,
            closed: false,
            add_path_errors: Vec::new(),
            rx_bufs: HashMap::new(),
            rx_frames: Vec::new(),
            rx_bytes: 0,
        }
    }

    /// Consume in-order stream data and record every frame that is complete.
    fn on_data(&mut self, stream_id: u64, data: &[u8], fin: bool) {
        let now = Instant::now();
        self.rx_bytes += data.len() as u64;

        let buf = self.rx_bufs.entry(stream_id).or_default();
        buf.extend_from_slice(data);
        let mut off = 0;
        while buf.len() - off >= FRAME_HDR_LEN {
            let hdr = &buf[off..off + FRAME_HDR_LEN];
            let seq = u32::from_be_bytes(hdr[0..4].try_into().unwrap());
            let len = u32::from_be_bytes(hdr[12..16].try_into().unwrap()) as usize;
            if buf.len() - off < FRAME_HDR_LEN + len {
                break;
            }
            self.rx_frames.push((seq, now));
            off += FRAME_HDR_LEN + len;
        }
        buf.drain(..off);
        if fin && buf.is_empty() {
            self.rx_bufs.remove(&stream_id);
        }
    }
}

struct Handler {
    side: Rc<RefCell<Side>>,
    buf: Vec<u8>,
}

impl TransportHandler for Handler {
    fn on_conn_created(&mut self, _conn: &mut Connection) {}

    fn on_conn_established(&mut self, conn: &mut Connection) {
        let mut side = self.side.borrow_mut();
        side.conn_idx = conn.index();
        side.established_at = Some(Instant::now());
        side.multipath = conn.is_multipath();
        if side.is_client && side.multipath {
            for (local, remote) in side.extra_paths.clone() {
                if let Err(e) = conn.add_path(local, remote) {
                    side.add_path_errors
                        .push(format!("{}->{}: {:?}", local, remote, e));
                }
            }
        }
    }

    fn on_conn_closed(&mut self, _conn: &mut Connection) {
        self.side.borrow_mut().closed = true;
    }

    fn on_stream_created(&mut self, _conn: &mut Connection, _stream_id: u64) {}

    fn on_stream_readable(&mut self, conn: &mut Connection, stream_id: u64) {
        while let Ok((n, fin)) = conn.stream_read(stream_id, &mut self.buf) {
            self.side
                .borrow_mut()
                .on_data(stream_id, &self.buf[..n], fin);
            if n == 0 || fin {
                break;
            }
        }
    }

    fn on_stream_writable(&mut self, _conn: &mut Connection, _stream_id: u64) {}

    fn on_stream_closed(&mut self, _conn: &mut Connection, _stream_id: u64) {}

    fn on_new_token(&mut self, _conn: &mut Connection, _token: Vec<u8>) {}
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum StreamMode {
    Single,
    PerFrame,
}

struct FrameRecord {
    gen: Instant,
    size: usize,
    is_iframe: bool,
}

/// Data written by the video source that the stream has not accepted yet.
struct PendingWrite {
    stream_id: Option<u64>,
    data: Bytes,
    fin: bool,
}

/// A constant bitrate video source with periodic I-frames.
struct VideoSource {
    mode: StreamMode,
    fps: u32,
    gop: u32,
    p_size: f64,
    i_size: f64,
    total_frames: u32,
    rng: StdRng,
    frames: Vec<FrameRecord>,
    pending: VecDeque<PendingWrite>,
    single_stream: Option<u64>,
}

impl VideoSource {
    fn new(opt: &SimOpt, mode: StreamMode) -> Self {
        let avg = opt.bitrate * 1e6 / 8.0 / opt.fps as f64;
        let gop = opt.gop.max(1) as f64;
        let p_size = avg * gop / (opt.iframe_ratio + gop - 1.0);
        VideoSource {
            mode,
            fps: opt.fps,
            gop: opt.gop.max(1),
            p_size,
            i_size: p_size * opt.iframe_ratio,
            total_frames: (opt.duration * opt.fps as f64).round() as u32,
            rng: StdRng::seed_from_u64(opt.seed ^ 0x5eed_f00d),
            frames: Vec::new(),
            pending: VecDeque::new(),
            single_stream: None,
        }
    }

    fn frame_interval(&self) -> Duration {
        Duration::from_secs_f64(1.0 / self.fps as f64)
    }

    /// Generate the next frame at time `gen` relative to `base`.
    fn generate(&mut self, gen: Instant, base: Instant) {
        let seq = self.frames.len() as u32;
        let is_iframe = seq % self.gop == 0;
        let nominal = if is_iframe { self.i_size } else { self.p_size };
        let payload_len = (nominal * self.rng.gen_range(0.9..1.1)).max(1.0) as usize;

        let mut data = vec![0u8; FRAME_HDR_LEN + payload_len];
        data[0..4].copy_from_slice(&seq.to_be_bytes());
        let gen_us = gen.saturating_duration_since(base).as_micros() as u64;
        data[4..12].copy_from_slice(&gen_us.to_be_bytes());
        data[12..16].copy_from_slice(&(payload_len as u32).to_be_bytes());

        self.frames.push(FrameRecord {
            gen,
            size: data.len(),
            is_iframe,
        });
        self.pending.push_back(PendingWrite {
            stream_id: None,
            data: Bytes::from(data),
            fin: self.mode == StreamMode::PerFrame,
        });
    }

    /// Hand pending frames to the transport until it stops accepting data.
    fn flush(&mut self, conn: &mut Connection) {
        while let Some(w) = self.pending.front_mut() {
            let stream_id = match w.stream_id {
                Some(id) => id,
                None => {
                    let id = match (self.mode, self.single_stream) {
                        (StreamMode::Single, Some(id)) => id,
                        _ => match conn.stream_uni_new(0, false) {
                            Ok(id) => id,
                            Err(_) => return, // stream limit, retry later
                        },
                    };
                    if self.mode == StreamMode::Single {
                        self.single_stream = Some(id);
                    }
                    w.stream_id = Some(id);
                    id
                }
            };

            match conn.stream_write(stream_id, w.data.clone(), w.fin) {
                Ok(n) if n >= w.data.len() => {
                    self.pending.pop_front();
                }
                Ok(n) => {
                    w.data = w.data.slice(n..);
                    return;
                }
                Err(tquic::error::Error::Done) => return,
                Err(e) => {
                    eprintln!("stream_write on stream {} failed: {:?}", stream_id, e);
                    self.pending.pop_front();
                }
            }
        }
    }
}

fn build_config(opt: &SimOpt, is_server: bool, multipath: Option<MultipathAlgorithm>) -> Config {
    let mut conf = Config::new().expect("config");
    conf.set_max_idle_timeout(opt.idle_timeout);
    conf.set_max_handshake_timeout(10_000);
    // Flow control windows of 15 MiB per connection and 6 MiB per stream,
    // which are the upper limits TQUIC uses by default. The limits must be
    // set before the initial values since the latter are capped by them.
    conf.set_max_connection_window(15 << 20);
    conf.set_max_stream_window(6 << 20);
    conf.set_initial_max_data(15 << 20);
    conf.set_initial_max_stream_data_bidi_local(6 << 20);
    conf.set_initial_max_stream_data_bidi_remote(6 << 20);
    conf.set_initial_max_stream_data_uni(6 << 20);
    conf.set_initial_max_streams_bidi(100);
    conf.set_initial_max_streams_uni(100_000);
    conf.set_active_connection_id_limit(8);
    conf.set_congestion_control_algorithm(
        CongestionControlAlgorithm::from_str(&opt.cc).expect("unknown congestion control"),
    );
    if opt.max_pto > 0 {
        conf.set_max_pto(opt.max_pto);
    }
    match multipath {
        Some(algor) => {
            conf.enable_multipath(true);
            conf.set_multipath_algorithm(algor);
        }
        None => conf.enable_multipath(false),
    }

    let alpn = vec![b"livesim".to_vec()];
    let tls = if is_server {
        TlsConfig::new_server_config(&opt.cert_file, &opt.key_file, alpn, false)
            .expect("server tls config")
    } else {
        let mut tls = TlsConfig::new_client_config(alpn, false).expect("client tls config");
        tls.set_verify(false);
        tls
    };
    conf.set_tls_config(tls);
    conf
}

/// Sleep until `t` with sub-millisecond accuracy.
fn sleep_until(t: Instant) {
    loop {
        let now = Instant::now();
        if now >= t {
            return;
        }
        let d = t - now;
        if d > Duration::from_micros(400) {
            std::thread::sleep(d - Duration::from_micros(250));
        } else {
            std::hint::spin_loop();
        }
    }
}

fn percentile(sorted: &[f64], q: f64) -> Value {
    if sorted.is_empty() {
        return Value::Null;
    }
    let idx = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    let v = sorted[idx];
    if v.is_finite() {
        json!((v * 1000.0).round() / 1000.0)
    } else {
        Value::Null
    }
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

fn main() {
    let opt = SimOpt::parse();
    let level = if opt.trace_on_spin {
        log::LevelFilter::Trace
    } else {
        opt.log_level
    };
    env_logger::builder()
        .filter_level(level)
        .format_timestamp_micros()
        .init();
    if opt.trace_on_spin {
        log::set_max_level(log::LevelFilter::Off);
    }

    let mode = opt.mode.to_ascii_lowercase();
    let multipath = match mode.as_str() {
        "single" => None,
        m => Some(MultipathAlgorithm::from_str(m).expect("unknown mode")),
    };
    let stream_mode = match opt.stream_mode.as_str() {
        "single" => StreamMode::Single,
        "frame" => StreamMode::PerFrame,
        m => panic!("unknown stream mode {}", m),
    };
    let downlink = match opt.direction.as_str() {
        "down" => true,
        "up" => false,
        d => panic!("unknown direction {}", d),
    };

    // Addresses: the client owns one address per path, the server one.
    let server_addr: SocketAddr = "10.100.0.1:443".parse().unwrap();
    let client_addrs: Vec<SocketAddr> = (0..opt.paths.len())
        .map(|i| format!("10.{}.0.2:5000", i + 1).parse().unwrap())
        .collect();

    // Emulated links: index 0 is client->server, index 1 is server->client.
    let mut links: Vec<[Link; 2]> = opt
        .paths
        .iter()
        .enumerate()
        .map(|(i, spec)| {
            let seed = opt.seed.wrapping_mul(1000).wrapping_add(i as u64 * 2);
            [
                Link::new(spec.clone(), seed),
                Link::new(spec.clone(), seed + 1),
            ]
        })
        .collect();

    let client_out = Rc::new(RefCell::new(Vec::new()));
    let server_out = Rc::new(RefCell::new(Vec::new()));
    let extra_paths = client_addrs[1..]
        .iter()
        .map(|a| (*a, server_addr))
        .collect::<Vec<_>>();
    let client_side = Rc::new(RefCell::new(Side::new(true, extra_paths)));
    let server_side = Rc::new(RefCell::new(Side::new(false, Vec::new())));

    let mut client = Endpoint::new(
        Box::new(build_config(&opt, false, multipath)),
        false,
        Box::new(Handler {
            side: client_side.clone(),
            buf: vec![0; 1 << 16],
        }),
        Rc::new(PacketCollector {
            out: client_out.clone(),
        }),
    );
    let mut server = Endpoint::new(
        Box::new(build_config(&opt, true, multipath)),
        true,
        Box::new(Handler {
            side: server_side.clone(),
            buf: vec![0; 1 << 16],
        }),
        Rc::new(PacketCollector {
            out: server_out.clone(),
        }),
    );

    let base = Instant::now();
    client
        .connect(
            client_addrs[0],
            server_addr,
            Some("example.org"),
            None,
            None,
            None,
        )
        .expect("connect");

    let (sender_side, receiver_side) = if downlink {
        (server_side.clone(), client_side.clone())
    } else {
        (client_side.clone(), server_side.clone())
    };

    let mut video = VideoSource::new(&opt, stream_mode);
    let frame_interval = video.frame_interval();
    let mut stream_start: Option<Instant> = None;
    let mut next_frame_at: Option<Instant> = None;
    let mut end_at: Option<Instant> = None;
    let mut arrivals: BinaryHeap<Arrival> = BinaryHeap::new();
    let mut arrival_seq: u64 = 0;
    let setup_deadline = base + Duration::from_secs(15);
    let mut aborted: Option<String> = None;
    let mut cc_rows: Vec<String> = Vec::new();
    let mut next_cc_sample = base;
    let mut iterations: u64 = 0;
    let mut spin_window = (Instant::now(), 0u64);
    let mut trace_until: Option<Instant> = None;
    let mut traced = false;

    loop {
        let now = Instant::now();
        iterations += 1;

        if opt.trace_on_spin {
            spin_window.1 += 1;
            if now >= spin_window.0 + Duration::from_millis(100) {
                if spin_window.1 > 20_000 && !traced {
                    eprintln!("spin detected: {} iterations in 100 ms", spin_window.1);
                    log::set_max_level(log::LevelFilter::Trace);
                    trace_until = Some(now + Duration::from_millis(20));
                    traced = true;
                }
                spin_window = (now, 0);
            }
            if trace_until.is_some_and(|t| now >= t) {
                log::set_max_level(log::LevelFilter::Off);
                trace_until = None;
            }
        }

        // Deliver the packets that have arrived.
        while arrivals.peek().is_some_and(|a| a.at <= now) {
            let mut a = arrivals.pop().unwrap();
            let info = PacketInfo {
                src: a.src,
                dst: a.dst,
                time: now,
            };
            let ep = if a.to_server {
                &mut server
            } else {
                &mut client
            };
            let _ = ep.recv(&mut a.data, &info);
        }

        client.on_timeout(now);
        server.on_timeout(now);

        // Start the live stream once the sender is established.
        if stream_start.is_none() {
            let established = sender_side.borrow().established_at;
            if let Some(t) = established {
                let start = t + Duration::from_secs_f64(opt.warmup);
                if now >= start {
                    stream_start = Some(start);
                    next_frame_at = Some(start);
                    end_at = Some(start + Duration::from_secs_f64(opt.duration + opt.drain));
                }
            } else if now > setup_deadline {
                aborted = Some("connection setup timed out".into());
                break;
            }
        }

        // Generate the frames that are due.
        while let Some(t) = next_frame_at {
            if t > now {
                break;
            }
            video.generate(t, base);
            next_frame_at = if (video.frames.len() as u32) < video.total_frames {
                Some(t + frame_interval)
            } else {
                None
            };
        }

        if !video.pending.is_empty() {
            let idx = sender_side.borrow().conn_idx;
            let ep = if downlink { &mut server } else { &mut client };
            if let Some(conn) = idx.and_then(|i| ep.conn_get_mut(i)) {
                video.flush(conn);
            }
        }

        let _ = client.process_connections();
        let _ = server.process_connections();

        if opt.cc_csv.is_some() && now >= next_cc_sample {
            next_cc_sample = now + CC_SAMPLE_INTERVAL;
            let idx = sender_side.borrow().conn_idx;
            let ep = if downlink { &mut server } else { &mut client };
            if let Some(conn) = idx.and_then(|i| ep.conn_get_mut(i)) {
                sample_cc(conn, &client_addrs, now, base, &mut cc_rows);
            }
        }

        // Put the outgoing packets on the emulated paths.
        for (out, to_server) in [(&client_out, true), (&server_out, false)] {
            for (data, info) in out.borrow_mut().drain(..) {
                let client_addr = if to_server { info.src } else { info.dst };
                let path = match client_addrs.iter().position(|a| *a == client_addr) {
                    Some(p) => p,
                    None => continue,
                };
                let dir = if to_server { 0 } else { 1 };
                if let Some(at) = links[path][dir].enqueue(now, data.len(), base, stream_start) {
                    arrival_seq += 1;
                    arrivals.push(Arrival {
                        at,
                        seq: arrival_seq,
                        to_server,
                        data,
                        src: info.src,
                        dst: info.dst,
                    });
                }
            }
        }

        if let Some(end) = end_at {
            if now >= end {
                break;
            }
        }
        if client_side.borrow().closed && server_side.borrow().closed {
            aborted = Some("connection closed".into());
            break;
        }

        // Sleep until the next event.
        let mut next = now + Duration::from_millis(50);
        let candidates = [
            arrivals.peek().map(|a| a.at),
            next_frame_at,
            end_at,
            client.timeout().map(|d| now + d),
            server.timeout().map(|d| now + d),
            if stream_start.is_none() {
                Some(now + Duration::from_millis(1))
            } else {
                None
            },
            opt.cc_csv.as_ref().map(|_| next_cc_sample),
        ];
        for t in candidates.into_iter().flatten() {
            next = next.min(t);
        }
        sleep_until(next);
    }

    let end = Instant::now();
    let client_paths = path_states(&mut client, client_side.borrow().conn_idx);
    let server_paths = path_states(&mut server, server_side.borrow().conn_idx);
    let mut summary = summarize(
        &opt,
        &mode,
        base,
        end,
        stream_start,
        &video,
        &sender_side,
        &receiver_side,
        if downlink { &mut server } else { &mut client },
        &links,
        aborted,
        downlink,
    );
    summary["client_paths"] = client_paths;
    summary["server_paths"] = server_paths;
    summary["loop_iterations"] = json!(iterations);

    let text = serde_json::to_string_pretty(&summary).unwrap();
    match &opt.out {
        Some(path) => std::fs::write(path, text).expect("write summary"),
        None => println!("{}", text),
    }

    if let Some(path) = &opt.frames_csv {
        write_frames_csv(
            path,
            &video,
            &receiver_side.borrow(),
            stream_start.unwrap_or(base),
        );
    }
    if let Some(path) = &opt.timeline_csv {
        write_timeline_csv(path, &links, base, stream_start.unwrap_or(base));
    }
    if let Some(path) = &opt.cc_csv {
        let offset_ms = stream_start
            .unwrap_or(base)
            .saturating_duration_since(base)
            .as_secs_f64()
            * 1000.0;
        let mut f = File::create(path).expect("create cc csv");
        writeln!(
            f,
            "t_ms,path,cwnd,pacing_rate,srtt_ms,min_rtt_ms,sent_bytes,acked_bytes,lost_bytes,pto_count,slow_start"
        )
        .unwrap();
        for row in &cc_rows {
            let (t, rest) = row.split_once(',').unwrap();
            let t: f64 = t.parse().unwrap();
            writeln!(f, "{:.1},{}", t - offset_ms, rest).unwrap();
        }
    }
}

/// Record the congestion state of every path of the sender connection.
fn sample_cc(
    conn: &mut Connection,
    client_addrs: &[SocketAddr],
    now: Instant,
    base: Instant,
    rows: &mut Vec<String>,
) {
    let t_ms = now.saturating_duration_since(base).as_secs_f64() * 1000.0;
    let tuples: Vec<_> = conn.paths_iter().collect();
    for t in tuples {
        let path = client_addrs
            .iter()
            .position(|a| *a == t.local || *a == t.remote)
            .map(|p| p as i64)
            .unwrap_or(-1);
        if let Ok(s) = conn.get_path_stats(t.local, t.remote) {
            rows.push(format!(
                "{:.1},{},{},{},{:.3},{:.3},{},{},{},{},{}",
                t_ms,
                path,
                s.final_cwnd,
                s.pacing_rate,
                s.srtt as f64 / 1000.0,
                s.min_rtt as f64 / 1000.0,
                s.sent_bytes,
                s.acked_bytes,
                s.lost_bytes,
                s.pto_count,
                s.in_slow_start as u8
            ));
        }
    }
}

/// Validation state and traffic counters of every path of a connection.
fn path_states(ep: &mut Endpoint, conn_idx: Option<u64>) -> Value {
    let conn = match conn_idx.and_then(|i| ep.conn_get_mut(i)) {
        Some(conn) => conn,
        None => return Value::Null,
    };
    let tuples: Vec<_> = conn.paths_iter().collect();
    let mut out = Vec::new();
    for t in tuples {
        let (active, state) = match conn.get_path(t.local, t.remote) {
            Ok(p) => (p.active(), format!("{:?}", p.state())),
            Err(_) => continue,
        };
        if let Ok(s) = conn.get_path_stats(t.local, t.remote) {
            out.push(json!({
                "local": t.local.to_string(),
                "remote": t.remote.to_string(),
                "active": active,
                "state": state,
                "sent_count": s.sent_count,
                "recv_count": s.recv_count,
                "acked_bytes": s.acked_bytes,
                "pto_count": s.pto_count,
            }));
        }
    }
    json!(out)
}

/// Completion time of each frame, indexed by sequence number.
fn frame_arrivals(video: &VideoSource, receiver: &Side) -> Vec<Option<Instant>> {
    let mut recv = vec![None; video.frames.len()];
    for &(seq, t) in &receiver.rx_frames {
        if let Some(slot) = recv.get_mut(seq as usize) {
            if slot.is_none() {
                *slot = Some(t);
            }
        }
    }
    recv
}

#[allow(clippy::too_many_arguments)]
fn summarize(
    opt: &SimOpt,
    mode: &str,
    base: Instant,
    end: Instant,
    stream_start: Option<Instant>,
    video: &VideoSource,
    sender: &Rc<RefCell<Side>>,
    receiver: &Rc<RefCell<Side>>,
    sender_ep: &mut Endpoint,
    links: &[[Link; 2]],
    aborted: Option<String>,
    downlink: bool,
) -> Value {
    let receiver = receiver.borrow();
    let sender = sender.borrow();
    let recv = frame_arrivals(video, &receiver);

    // Frame latency in ms; frames that never completed are infinite.
    let latencies: Vec<f64> = video
        .frames
        .iter()
        .zip(&recv)
        .map(|(f, r)| match r {
            Some(t) => t.saturating_duration_since(f.gen).as_secs_f64() * 1000.0,
            None => f64::INFINITY,
        })
        .collect();
    let mut sorted = latencies.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let received: Vec<f64> = latencies
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .collect();
    let n = latencies.len().max(1) as f64;
    let mean = if received.is_empty() {
        Value::Null
    } else {
        json!(round3(received.iter().sum::<f64>() / received.len() as f64))
    };

    // A player that skips frames missing the deadline.
    let late: serde_json::Map<String, Value> = DEADLINES_MS
        .iter()
        .map(|d| {
            let cnt = latencies.iter().filter(|&&v| v > *d as f64).count();
            (d.to_string(), json!(round3(cnt as f64 / n)))
        })
        .collect();

    // A player that starts with a fixed buffer and stalls until the next
    // frame arrives. Stalls shift the playout time of all later frames.
    let video_secs = video.frames.len() as f64 / video.fps as f64;
    let rebuffer: serde_json::Map<String, Value> = DEADLINES_MS
        .iter()
        .map(|b| {
            let mut shift = Duration::ZERO;
            let mut count = 0u64;
            for (f, r) in video.frames.iter().zip(&recv) {
                let due = f.gen + Duration::from_millis(*b) + shift;
                let arrived = r.unwrap_or(end).max(due);
                if arrived > due {
                    count += 1;
                    shift += arrived - due;
                }
            }
            let stall = shift.as_secs_f64();
            (
                b.to_string(),
                json!({
                    "stalls": count,
                    "stall_s": round3(stall),
                    "stall_ratio": round3(stall / video_secs.max(1e-9)),
                }),
            )
        })
        .collect();

    // Transport statistics of the sender.
    let mut paths_json = Vec::new();
    let mut conn_json = Value::Null;
    if let Some(conn) = sender.conn_idx.and_then(|i| sender_ep.conn_get_mut(i)) {
        let s = conn.stats();
        conn_json = json!({
            "sent_count": s.sent_count,
            "sent_bytes": s.sent_bytes,
            "recv_count": s.recv_count,
            "lost_count": s.lost_count,
            "lost_bytes": s.lost_bytes,
            "is_multipath": conn.is_multipath(),
        });
        let tuples: Vec<_> = conn.paths_iter().collect();
        for t in tuples {
            let (active, state) = match conn.get_path(t.local, t.remote) {
                Ok(p) => (p.active(), format!("{:?}", p.state())),
                Err(_) => (false, String::new()),
            };
            if let Ok(ps) = conn.get_path_stats(t.local, t.remote) {
                paths_json.push(json!({
                    "local": t.local.to_string(),
                    "remote": t.remote.to_string(),
                    "active": active,
                    "state": state,
                    "sent_count": ps.sent_count,
                    "sent_bytes": ps.sent_bytes,
                    "lost_count": ps.lost_count,
                    "lost_bytes": ps.lost_bytes,
                    "acked_bytes": ps.acked_bytes,
                    "srtt_ms": round3(ps.srtt as f64 / 1000.0),
                    "min_rtt_ms": round3(ps.min_rtt as f64 / 1000.0),
                    "final_cwnd": ps.final_cwnd,
                    "pto_count": ps.pto_count,
                }));
            }
        }
    }

    let app_bytes: u64 = video.frames.iter().map(|f| f.size as u64).sum();
    let sender_dir = if downlink { 1 } else { 0 };
    let wire_bytes: u64 = links.iter().map(|l| l[sender_dir].stats.bytes_in).sum();

    json!({
        "label": opt.label,
        "mode": mode,
        "cc": opt.cc,
        "direction": opt.direction,
        "stream_mode": opt.stream_mode,
        "bitrate_mbps": opt.bitrate,
        "fps": opt.fps,
        "gop": opt.gop,
        "iframe_ratio": opt.iframe_ratio,
        "duration_s": opt.duration,
        "seed": opt.seed,
        "max_pto_ms": opt.max_pto,
        "paths": opt.paths.iter().map(|p| p.to_json()).collect::<Vec<_>>(),
        "aborted": aborted,
        "stream_start_ms": stream_start.map(|t| round3((t - base).as_secs_f64() * 1000.0)),
        "multipath_negotiated": sender.multipath,
        "add_path_errors": receiver.add_path_errors.iter().chain(&sender.add_path_errors).collect::<Vec<_>>(),
        "frames": {
            "generated": video.frames.len(),
            "received": received.len(),
            "missing": video.frames.len() - received.len(),
        },
        "latency_ms": {
            "mean_received": mean,
            "p50": percentile(&sorted, 0.50),
            "p90": percentile(&sorted, 0.90),
            "p95": percentile(&sorted, 0.95),
            "p99": percentile(&sorted, 0.99),
            "max": percentile(&sorted, 1.0),
        },
        "late_ratio": late,
        "rebuffer": rebuffer,
        "goodput_mbps": round3(receiver.rx_bytes as f64 * 8.0 / 1e6 / video_secs.max(1e-9)),
        "app_bytes": app_bytes,
        "sender_wire_bytes": wire_bytes,
        "wire_overhead": round3(wire_bytes as f64 / (app_bytes.max(1)) as f64),
        "sender_conn": conn_json,
        "sender_paths": paths_json,
        "links": links.iter().map(|l| json!({
            "up": l[0].stats_json(),
            "down": l[1].stats_json(),
        })).collect::<Vec<_>>(),
    })
}

fn write_frames_csv(path: &str, video: &VideoSource, receiver: &Side, start: Instant) {
    let recv = frame_arrivals(video, receiver);
    let mut f = File::create(path).expect("create frames csv");
    writeln!(f, "seq,gen_ms,size,iframe,recv_ms,latency_ms").unwrap();
    for (i, (fr, r)) in video.frames.iter().zip(&recv).enumerate() {
        let gen_ms = fr.gen.saturating_duration_since(start).as_secs_f64() * 1000.0;
        match r {
            Some(t) => {
                let recv_ms = t.saturating_duration_since(start).as_secs_f64() * 1000.0;
                writeln!(
                    f,
                    "{},{:.3},{},{},{:.3},{:.3}",
                    i,
                    gen_ms,
                    fr.size,
                    fr.is_iframe as u8,
                    recv_ms,
                    recv_ms - gen_ms
                )
                .unwrap();
            }
            None => {
                writeln!(
                    f,
                    "{},{:.3},{},{},,",
                    i, gen_ms, fr.size, fr.is_iframe as u8
                )
                .unwrap();
            }
        }
    }
}

fn write_timeline_csv(path: &str, links: &[[Link; 2]], base: Instant, start: Instant) {
    let offset_ms = start.saturating_duration_since(base).as_millis() as i64;
    let mut f = File::create(path).expect("create timeline csv");
    writeln!(f, "t_ms,path,dir,offered_bytes,delivered_bytes").unwrap();
    for (p, pair) in links.iter().enumerate() {
        for (d, link) in pair.iter().enumerate() {
            for (i, (offered, delivered)) in link.timeline.iter().enumerate() {
                let t = i as i64 * TIMELINE_BUCKET.as_millis() as i64 - offset_ms;
                writeln!(
                    f,
                    "{},{},{},{},{}",
                    t,
                    p,
                    if d == 0 { "up" } else { "down" },
                    offered,
                    delivered
                )
                .unwrap();
            }
        }
    }
}
