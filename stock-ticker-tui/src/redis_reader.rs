use std::{
    collections::HashMap,
    process,
    sync::mpsc::{self, Sender, Receiver},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use color_eyre::eyre::eyre;
use redis::Connection;

use crate::{ItchyMessage, Packet, Trade, reader::PacketReader};

fn group_name() -> String {
    format!("stock-ticker-tui-{}", process::id())
}

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

        let group = group_name();

        let _: Result<(), _> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream_key)
            .arg(&group)
            .arg("0")
            .arg("MKSTREAM")
            .query(&mut con);

        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            drain_pending(&mut con, &self.stream_key, &group, &tx);

            loop {
                let reply: redis::streams::StreamReadReply = match redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(&group)
                    .arg(&group)
                    .arg("BLOCK")
                    .arg(100)
                    .arg("COUNT")
                    .arg(1000)
                    .arg("STREAMS")
                    .arg(&self.stream_key)
                    .arg(">")
                    .query(&mut con)
                {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                if !process_and_ack(&mut con, &self.stream_key, &group, &reply, &tx) {
                    return;
                }
            }
        });

        Ok(rx)
    }
}

/// Reads all pending (unacknowledged) entries from a previous run, sends them
/// over the channel, and ACKs them before returning.
fn drain_pending(
    con: &mut Connection,
    stream_key: &str,
    group: &str,
    tx: &Sender<color_eyre::Result<Packet>>,
) {
    let mut pending_start = "0-0".to_string();
    loop {
        let reply: redis::streams::StreamReadReply = match redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group)
            .arg(group)
            .arg("COUNT")
            .arg(1000)
            .arg("STREAMS")
            .arg(stream_key)
            .arg(&pending_start)
            .query(con)
        {
            Ok(r) => r,
            Err(_) => return,
        };

        let has_entries = reply.keys.iter().any(|k| !k.ids.is_empty());
        if !has_entries {
            return;
        }

        for stream_key_entry in &reply.keys {
            if let Some(last) = stream_key_entry.ids.last() {
                pending_start = last.id.clone();
            }
        }

        if !process_and_ack(con, stream_key, group, &reply, tx) {
            return;
        }
    }
}

/// Parses entries, sends packets over the channel, and batch-ACKs.
/// Returns false if the channel is closed.
fn process_and_ack(
    con: &mut Connection,
    stream_key: &str,
    group: &str,
    reply: &redis::streams::StreamReadReply,
    tx: &Sender<color_eyre::Result<Packet>>,
) -> bool {
    let mut ack_ids: Vec<String> = Vec::new();

    for sk in &reply.keys {
        for entry in &sk.ids {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros();

            ack_ids.push(entry.id.clone());
            let pairs = to_string_map(&entry.map);
            let result = parse_packet(pairs, now);

            if tx.send(result).is_err() {
                return false;
            }
        }
    }

    if !ack_ids.is_empty() {
        let mut cmd = redis::cmd("XACK");
        cmd.arg(stream_key).arg(group);
        for id in &ack_ids {
            cmd.arg(id);
        }
        let _: Result<(), _> = cmd.query(con);
    }

    true
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
