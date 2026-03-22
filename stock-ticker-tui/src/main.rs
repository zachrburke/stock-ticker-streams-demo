use std::{
    collections::HashMap, fmt, fs, net::{Ipv4Addr, UdpSocket},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use color_eyre::eyre::{Context, eyre};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::{
    DefaultTerminal, Frame,
    style::Stylize,
    text::Line,
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let terminal = ratatui::init();
    let result = App::new()?.run(terminal);
    ratatui::restore();
    result
}

const TOP_STOCKS: &[&str] = &[
    "AAPL", // Apple
    "MSFT", // Microsfot
    "TSLA", // Tesla
    "GOOG", // Google
    "AMZN", // Amazon
    "F", // Ford (common options stock)
    "GME", // GameStop
];
const WATCHLIST: &[&str] = &[
    "PEP",   // PepsiCo
    "COST",  // Costco
    "CSCO",  // Cisco
    "INTC",  // Intel
    "CMCSA", // Comcast
    "TMUS",  // T-Mobile
    "GILD",  // Gilead
    "FISV",  // Fiserv
    "PAYX",  // Paychex
    "XLNX",  // Xilinx
    "SIRI",  // SiriusXM
    "KHC",   // Kraft Heinz
    "MDLZ",  // Mondelez
    "WBA",   // Walgreens
    "XRAY",  // Dentsply Sirona
];

pub enum ItchyMessage {
    StockTick(Trade),
    Reset,
}

pub struct Trade {
    price: f32,
    symbol: String,
}

impl Trade {
    pub fn try_from_hashmap(map: &HashMap<&str, &str>) -> color_eyre::Result<Trade> {
        let symbol = map.get("symbol").ok_or_else(|| eyre!("missing: symbol"))?;
        let price_raw = map.get("price").ok_or_else(|| eyre!("missing: price"))?;
        Ok(Trade {
            symbol: symbol.to_string(),
            price: price_raw.parse()?,
        })
    }
}

const LATENCY_BATCH: u128 = 1000;

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct Stats {
    pub last_seq: u32,
    pub drop_count: u32,
    pub total_count: u32,
    pub latency_us: u128,
    latency_sum: u128,
    latency_count: u128,
}

impl Stats {
    /// Updates seq and drop count. Returns true if seq=0 (reset).
    pub fn update(&mut self, seq: u32) -> bool {
        if seq == 0 {
            *self = Self::default();
            return true;
        }
        if seq > self.last_seq + 1 {
            self.drop_count += seq - (self.last_seq + 1);
        }
        self.last_seq = seq;
        self.total_count += 1;
        false
    }

    pub fn record_latency(&mut self, latency_us: u128) {
        self.latency_sum += latency_us;
        self.latency_count += 1;
        if self.latency_count >= LATENCY_BATCH {
            self.latency_us = self.latency_sum / self.latency_count;
            self.latency_sum = 0;
            self.latency_count = 0;
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total:{},last:{},dropped:{},latency:{}us",
            self.total_count, self.last_seq, self.drop_count, self.latency_us
        )
    }
}

/// Spawns a reader thread that drains UDP as fast as possible and sends
/// parsed messages over a channel. Latency is measured here, at read time,
/// not when the TUI processes the message.
struct Packet {
    msg: ItchyMessage,
    seq: u32,
    latency_us: u128,
}

fn spawn_reader() -> std::io::Result<Receiver<color_eyre::Result<Packet>>> {
    let socket = UdpSocket::bind("0.0.0.0:1234")?;
    socket.join_multicast_v4(
        &"239.255.0.1".parse::<Ipv4Addr>().unwrap(),
        &Ipv4Addr::UNSPECIFIED,
    )?;

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
                let seq: u32 = match pairs.get("seq").copied() {
                    Some(s) => s.parse()?,
                    _ => return Err(eyre!("missing seq")),
                };
                let sender_ts: u128 = match pairs.get("ts").copied() {
                    Some(s) => s.parse()?,
                    _ => return Err(eyre!("missing ts")),
                };
                let msg = match pairs.get("kind").copied() {
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

fn parse_message(s: &str) -> HashMap<&str, &str> {
    s.split(';')
        .filter_map(|pair| pair.split_once('='))
        .collect()
}

/// The main application which holds the state and logic of the application.
pub struct App {
    /// Is the application running?
    running: bool,
    rx: Receiver<color_eyre::Result<Packet>>,
    stats: Stats,
    symbols: HashMap<String, f32>,
}

impl App {
    pub fn new() -> std::io::Result<Self> {
        let rx = spawn_reader()?;
        Ok(Self {
            running: true,
            rx,
            stats: Stats::default(),
            symbols: HashMap::<String, f32>::new(),
        })
    }

    /// Run the application's main loop.
    pub fn run(mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        self.running = true;
        let _ = self.load_symbols();
        let _ = self.load_stats();
        while self.running {
            for result in self.rx.try_iter() {
                let packet = result?;
                if self.stats.update(packet.seq) {
                    self.symbols.clear();
                    continue;
                }
                self.stats.record_latency(packet.latency_us);
                match packet.msg {
                    ItchyMessage::StockTick(trade) => {
                        self.symbols.insert(trade.symbol.to_owned(), trade.price);
                    }
                    ItchyMessage::Reset => {
                        self.symbols.clear();
                    }
                }
            }
            terminal.draw(|frame| self.render(frame))?;
            self.handle_crossterm_events()?;
        }
        Ok(())
    }

    fn persist_symbols(&self) -> color_eyre::Result<()> {
        fs::create_dir_all(".data")?;
        let bytes = rmp_serde::to_vec(&self.symbols)?;
        fs::write(".data/snapshot.msgpack", bytes)?;
        Ok(())
    }

    fn persist_stats(&self) -> color_eyre::Result<()> {
        let bytes = rmp_serde::to_vec(&self.stats)?;
        fs::write(".data/stats.msgpack", bytes)?;
        Ok(())
    }

    fn load_symbols(&mut self) -> color_eyre::Result<()> {
        let bytes = fs::read(".data/snapshot.msgpack")?;
        self.symbols = rmp_serde::from_slice(&bytes)?;
        Ok(())
    }

    fn load_stats(&mut self) -> color_eyre::Result<()> {
        let bytes = fs::read(".data/stats.msgpack")?;
        self.stats = rmp_serde::from_slice(&bytes)?;
        Ok(())
    }

    fn render(&mut self, frame: &mut Frame) {
        let outer_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Percentage(80), Constraint::Percentage(20)])
            .split(frame.area());

        let inner_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(outer_layout[0]);

        let title = Line::from("Ratatui Simple Stock Ticker")
            .bold()
            .blue()
            .centered();
        let status_txt = self.stats.to_string();
        let text = format!(
            "{status_txt}
            Press `Esc`, `Ctrl-C` or `q` to stop running."
        );
        frame.render_widget(
            Paragraph::new(text)
                .block(Block::bordered().title(title))
                .centered(),
            outer_layout[1],
        );

        frame.render_widget(self.watch_list_widget(" Top Stocks ", TOP_STOCKS), inner_layout[0]);
        frame.render_widget(self.watch_list_widget(" Watchlist ", WATCHLIST), inner_layout[1]);
    }

    fn watch_list_widget(&self, title: &str, list: &[&str]) -> List<'_> {
        let items: Vec<ListItem> = self
            .symbols
            .iter()
            .filter(|(sym, _)| list.contains(&sym.as_str()))
            .map(|(symbol, price)| ListItem::new(format!("{symbol:<6} ${price}")))
            .collect();

        List::new(items).block(Block::default().title(title.to_string()).borders(Borders::ALL))
    }

    /// Reads the crossterm events and updates the state of [`App`].
    fn handle_crossterm_events(&mut self) -> color_eyre::Result<()> {
        if event::poll(Duration::ZERO)? {
            match event::read()? {
                // it's important to check KeyEventKind::Press to avoid handling key release events
                Event::Key(key) if key.kind == KeyEventKind::Press => self.on_key_event(key),
                Event::Mouse(_) => {}
                Event::Resize(_, _) => {}
                _ => {}
            }
        }
        Ok(())
    }

    /// Handles the key events and updates the state of [`App`].
    fn on_key_event(&mut self, key: KeyEvent) {
        match (key.modifiers, key.code) {
            (_, KeyCode::Esc | KeyCode::Char('q'))
            | (KeyModifiers::CONTROL, KeyCode::Char('c') | KeyCode::Char('C')) => self.quit(),
            // Add other key handlers here.
            _ => {}
        }
    }

    /// Set running to false to quit the application.
    fn quit(&mut self) {
        let _ = self.persist_symbols();
        let _ = self.persist_stats();
        self.running = false;
    }
}
