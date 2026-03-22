use std::sync::mpsc::Receiver;

use crate::Packet;

pub trait PacketReader {
    fn spawn(self) -> color_eyre::Result<Receiver<color_eyre::Result<Packet>>>;
}
