# TQUIC tools

[TQUIC](https://github.com/Tencent/tquic) is a high-performance, lightweight, and cross-platform library for the [IETF QUIC](https://datatracker.ietf.org/wg/quic/bout/) protocol. 

The crate contains client and server tools based on TQUIC:
- tquic_client: A QUIC and HTTP/3 client. It's also an HTTP/3 benchmarking tool.
- tquic_server: A QUIC and HTTP/3 static file server.
- tquic_mpsim: An in-process multipath network emulator that streams live video
  over a TQUIC connection and reports frame latency and rebuffering.


## Multipath live streaming emulation

`tquic_mpsim` runs a client and a server in one process and connects them
through emulated paths. Each `--path` sets a bottleneck bandwidth (`bw`, Mbit/s),
a one-way delay (`delay`, ms), a drop-tail queue (`queue`, ms), random loss
(`loss`, percent), bursty loss (`burst`, mean burst length in packets) and
outage windows (`outage=<start>-<end>` in seconds after the stream starts).
The emulation runs in real time.

```
cargo build --release -p tquic_tools --bin tquic_mpsim

# WiFi-like path with 5% loss plus a clean LTE-like path, MinRTT scheduler
target/release/tquic_mpsim --mode minrtt --cc cubic \
    --path bw=20,delay=15,loss=5 --path bw=10,delay=30 \
    --bitrate 4 --fps 30 --duration 30 --frames-csv frames.csv
```

`--mode` is `single` (multipath disabled), `minrtt`, `redundant` or
`roundrobin`. The JSON summary contains the frame latency percentiles, the
ratio of frames later than several playout deadlines, a rebuffering model for
several startup buffers and the per-path statistics of both endpoints.

`tools/script/mpsim_matrix.py` runs the experiment matrices used to evaluate
the schedulers (random loss, bursty loss and outages) and aggregates them into
a CSV file.


## Installation

```
cargo install tquic_tools
```


## Documentation

- [English version](https://tquic.net/docs/getting_started/demo/)
- [Chinese version](https://tquic.net/zh/docs/getting_started/demo/)


## License

The project is under the Apache 2.0 license.
