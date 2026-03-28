mod reader;
mod udp_reader;
mod redis_reader;

use std::{
    collections::HashMap, fmt, fs,
    sync::mpsc::Receiver,
    time::Duration,
};

use cadence::prelude::*;
use cadence::{StatsdClient, UdpMetricSink, DEFAULT_PORT};
use clap::Parser;
use color_eyre::eyre::eyre;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::{
    DefaultTerminal, Frame,
    style::Stylize,
    text::Line,
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

use reader::PacketReader;
use udp_reader::UdpMulticastReader;
use redis_reader::RedisStreamReader;

#[derive(Parser)]
struct Cli {
    /// Source to read from: "udp" or "redis"
    #[arg(long, default_value = "udp")]
    source: String,
}

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let rx = match cli.source.as_str() {
        "udp" => UdpMulticastReader::default().spawn()?,
        "redis" => RedisStreamReader::default().spawn()?,
        other => return Err(eyre!("unknown source: {other}")),
    };
    let terminal = ratatui::init();
    let result = App::new(rx, &cli.source).run(terminal);
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
    pub price: f32,
    pub symbol: String,
}

impl Trade {
    pub fn try_from_hashmap(map: &HashMap<String, String>) -> color_eyre::Result<Trade> {
        let symbol = map.get("symbol").ok_or_else(|| eyre!("missing: symbol"))?;
        let price_raw = map.get("price").ok_or_else(|| eyre!("missing: price"))?;
        Ok(Trade {
            symbol: symbol.to_string(),
            price: price_raw.parse()?,
        })
    }
}

const LATENCY_BATCH: u128 = 1000;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct Stats {
    pub last_seq: u32,
    pub drop_count: u32,
    pub total_count: u32,
    pub latency_ns: u128,
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
            self.latency_ns = self.latency_sum / self.latency_count;
            self.latency_sum = 0;
            self.latency_count = 0;
        }
    }
}

fn new_statsd_client(source: &str) -> StatsdClient {
    let host = ("0.0.0.0", DEFAULT_PORT);
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let sink = UdpMetricSink::from(host, socket).unwrap();
    StatsdClient::from_sink(&format!("ticker.{source}"), sink)
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total:{},last:{},dropped:{},latency:{}ns",
            self.total_count, self.last_seq, self.drop_count, self.latency_ns
        )
    }
}

pub struct Packet {
    pub msg: ItchyMessage,
    pub seq: u32,
    pub latency_us: u128,
}

/// The main application which holds the state and logic of the application.
pub struct App {
    /// Is the application running?
    running: bool,
    rx: Receiver<color_eyre::Result<Packet>>,
    stats: Stats,
    statsd: StatsdClient,
    symbols: HashMap<String, f32>,
}

impl App {
    pub fn new(rx: Receiver<color_eyre::Result<Packet>>, source: &str) -> Self {
        Self {
            running: true,
            rx,
            stats: Stats::default(),
            statsd: new_statsd_client(source),
            symbols: HashMap::<String, f32>::new(),
        }
    }

    /// Run the application's main loop.
    pub fn run(mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        self.running = true;
        let _ = self.load_symbols();
        let _ = self.load_stats();
        while self.running {
            for result in self.rx.try_iter() {
                let packet = result?;
                let prev_drop_count = self.stats.drop_count;
                if self.stats.update(packet.seq) {
                    self.symbols.clear();
                    continue;
                }
                let new_drops = self.stats.drop_count - prev_drop_count;
                if new_drops > 0 {
                    let _ = self.statsd.count("dropped", new_drops as i64);
                }
                let _ = self.statsd.count("total", 1);
                let _ = self.statsd.time("latency_us", packet.latency_us as u64);
                self.stats.record_latency(packet.latency_us);
                if self.stats.latency_count == 0 {
                    let _ = self.statsd.gauge("latency_avg_us", self.stats.latency_ns as u64);
                }
                match packet.msg {
                    ItchyMessage::StockTick(trade) => {
                        self.symbols.insert(trade.symbol.to_owned(), trade.price);
                    }
                    ItchyMessage::Reset => {
                        self.symbols.clear();
                    }
                }
            }
            let _ = self.persist_symbols();
            let _ = self.persist_stats();
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

        let title = Line::from("Simple Stock Ticker")
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
        let mut pairs: Vec<(&String, &f32)> = self
            .symbols
            .iter()
            .filter(|(sym, _)| list.contains(&sym.as_str()))
            .collect();
        pairs.sort_by_key(|(sym, _)| sym.as_str());
        let items: Vec<ListItem> = pairs
            .into_iter()
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
