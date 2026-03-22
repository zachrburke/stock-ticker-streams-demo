use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use color_eyre::eyre::eyre;

use crate::{ItchyMessage, Packet, Trade, reader::PacketReader};

const GROUP: &str = "stock-ticker-tui";
const CONSUMER: &str = "tui-1";

pub struct RedisStreamReader {
    pub url: String,
    pub stream_key: String,
}

impl Default for RedisStreamReader {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1/".to_string(),
            stream_key: "stock:ticks".to_string(),
        }
    }
}

impl PacketReader for RedisStreamReader {
    fn spawn(self) -> color_eyre::Result<Receiver<color_eyre::Result<Packet>>> {
        let client = redis::Client::open(self.url.as_str())?;
        let mut con = client.get_connection()?;

        let _: Result<(), _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream_key)
            .arg(GROUP)
            .arg("0")
            .arg("MKSTREAM")
            .query(&mut con);

        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            // First drain pending (unacknowledged) messages from a previous run
            let mut reading_pending = true;
            let mut pending_start = "0-0".to_string();

            loop {
                let id_arg = if reading_pending { pending_start.as_str() } else { ">" };

                let reply: redis::streams::StreamReadReply = match redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(GROUP)
                    .arg(CONSUMER)
                    .arg("BLOCK")
                    .arg(100)
                    .arg("COUNT")
                    .arg(1000)
                    .arg("STREAMS")
                    .arg(&self.stream_key)
                    .arg(id_arg)
                    .query(&mut con)
                {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                if reading_pending {
                    let has_entries = reply.keys.iter().any(|k| !k.ids.is_empty());
                    if !has_entries {
                        reading_pending = false;
                        continue;
                    }
                }

                for stream_key in &reply.keys {
                    for entry in &stream_key.ids {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_micros();

                        if reading_pending {
                            pending_start = entry.id.clone();
                        }

                        let pairs = to_string_map(&entry.map);
                        let result = parse_packet(pairs, now);

                        let _: Result<(), _> = redis::cmd("XACK")
                            .arg(&self.stream_key)
                            .arg(GROUP)
                            .arg(&entry.id)
                            .query(&mut con);

                        if tx.send(result).is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }
}

fn extract_str(value: &redis::Value) -> Option<String> {
    match value {
        redis::Value::BulkString(bytes) => String::from_utf8(bytes.clone()).ok(),
        redis::Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

fn to_string_map(map: &HashMap<String, redis::Value>) -> HashMap<String, String> {
    map.iter()
        .filter_map(|(k, v)| extract_str(v).map(|s| (k.clone(), s)))
        .collect()
}

fn parse_packet(
    pairs: HashMap<String, String>,
    now: u128,
) -> color_eyre::Result<Packet> {
    let seq: u32 = match pairs.get("seq") {
        Some(s) => s.parse()?,
        _ => return Err(eyre!("missing seq")),
    };
    let sender_ts: u128 = match pairs.get("ts") {
        Some(s) => s.parse()?,
        _ => return Err(eyre!("missing ts")),
    };
    let msg = match pairs.get("kind").map(|s| s.as_str()) {
        Some("tick") => Trade::try_from_hashmap(&pairs).map(ItchyMessage::StockTick)?,
        _ if seq == 0 => ItchyMessage::Reset,
        other => return Err(eyre!("unknown kind: {:?}", other)),
    };

    Ok(Packet {
        msg,
        seq,
        latency_us: now.saturating_sub(sender_ts),
    })
}
