use std::{net::UdpSocket, thread::sleep, time::Duration};

use rust_decimal::Decimal;

const MULTICAST_ADDR: &str = "239.255.0.1:1234";
const MARKET_OPEN_NS: u64 = 34_200_000_000_000;

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;

    let stream = itchy::MessageStream::from_file("12302019.NASDAQ_ITCH50")
        .expect("failed to read file with itchy, perhaps the file is missing.");

    // Start here, we'll sleep for the difference in ns between messages
    // and update this to keep track of where we are.
    let mut duration = MARKET_OPEN_NS.clone();

    for msg in stream {
        let Ok(actual_msg) = msg else {
            continue;
        };
        if actual_msg.timestamp > duration {
            let delay = actual_msg.timestamp - duration;
            // Simulate realistic time.. could divide this by a value
            // to fast forward at 2x.. 4x.. etc.
            sleep(Duration::from_nanos(delay));
            duration = actual_msg.timestamp;
        }
        match actual_msg.body {
            itchy::Body::NonCrossTrade(trade) => {
                let datagram = format!(
                    "kind=tick;symbol={};price={:?}",
                    trade.stock.trim(),
                    Decimal::from(trade.price),
                );
                socket.send_to(datagram.as_bytes(), MULTICAST_ADDR)?;
            }
            _ => continue,
        }
    }

    Ok(())
}
