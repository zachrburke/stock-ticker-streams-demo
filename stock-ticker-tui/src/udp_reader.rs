use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    sync::mpsc::{self, Receiver},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use color_eyre::eyre::{Context, eyre};
use socket2::{Domain, Protocol, Socket, Type};

use crate::{ItchyMessage, Packet, Trade, reader::PacketReader};

pub struct UdpMulticastReader {
    pub addr: Ipv4Addr,
    pub port: u16,
}

impl Default for UdpMulticastReader {
    fn default() -> Self {
        Self {
            addr: Ipv4Addr::new(239, 255, 0, 1),
            port: 1234,
        }
    }
}

impl PacketReader for UdpMulticastReader {
    fn spawn(self) -> color_eyre::Result<Receiver<color_eyre::Result<Packet>>> {
        let socket2 = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket2.set_reuse_port(true)?;
        socket2.set_reuse_address(true)?;
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", self.port).parse()?;
        socket2.bind(&bind_addr.into())?;
        let socket: UdpSocket = socket2.into();
        socket.join_multicast_v4(&self.addr, &Ipv4Addr::UNSPECIFIED)?;

        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                let Ok((len, _)) = socket.recv_from(&mut buf) else {
                    continue;
                };
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros();
                let datagram = String::from_utf8_lossy(&buf[..len]).to_string();
                let pairs = parse_message(&datagram);

                let result = (|| -> color_eyre::Result<Packet> {
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
                })()
                .wrap_err_with(|| format!("raw datagram: {datagram}"));

                if tx.send(result).is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

fn parse_message(s: &str) -> HashMap<String, String> {
    s.split(';')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
