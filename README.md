### Presentation on how you might acheive a low latency fan-out of market data using redis streams

Slides can be viewed using [presenterm](https://github.com/mfontanini/presenterm).

```
presenterm -x presentation.md
```

Before running the examples in the slides, you will need to run the senders/transmitters.


For UDP 
```
cd itchy-transmitter
cargo run
```

For Redis Streams
```
cd itchy-transmitter-redis
cargo run
```

To run either of them, you'll need an archive of NASDAQ data in ITCH format. You can download the one I used [here](https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/12302019.NASDAQ_ITCH50.gz)

To run the redis transmitter you will need to run redis-server, ideally on your local machine.  You can run it in a docker container but the latency will be much higher.

Credit goes to [itchy-rust](https://github.com/adwhit/itchy-rust?tab=readme-ov-file) for making it possible to simulate market data from NASDAQ.





