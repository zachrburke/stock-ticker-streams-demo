use std::{thread::sleep, time::{Duration, SystemTime, UNIX_EPOCH}};

use rust_decimal::Decimal;

const STREAM_KEY: &str = "stock:ticks";
const MARKET_OPEN_NS: u64 = 34_200_000_000_000;

// Higher number means messages get sent faster
const SPEED: u64 = 1;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_connection()?;

    // Reset the stream on startup
    redis::cmd("DEL").arg(STREAM_KEY).exec(&mut con).unwrap();

    let stream = itchy::MessageStream::from_file("12302019.NASDAQ_ITCH50")
        .expect("failed to read file with itchy, perhaps the file is missing.");

    let mut duration = MARKET_OPEN_NS.clone();
    let mut sequence: u32 = 0;

    for msg in stream {
        let Ok(actual_msg) = msg else {
            continue;
        };
        if actual_msg.timestamp > duration && SPEED < 10 {
            let delay = actual_msg.timestamp - duration;
            sleep(Duration::from_nanos(delay / SPEED));
            duration = actual_msg.timestamp;
        }
        match actual_msg.body {
            itchy::Body::NonCrossTrade(trade) => {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                redis::cmd("XADD")
                    .arg(STREAM_KEY)
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(100_000)
                    .arg("*")
                    .arg("kind")
                    .arg("tick")
                    .arg("symbol")
                    .arg(trade.stock.trim())
                    .arg("price")
                    .arg(format!("{:?}", Decimal::from(trade.price)))
                    .arg("seq")
                    .arg(sequence)
                    .arg("ts")
                    .arg(ts.to_string())
                    .exec(&mut con).unwrap();
                sequence += 1;
            }
            _ => continue,
        }
    }

    Ok(())
}
