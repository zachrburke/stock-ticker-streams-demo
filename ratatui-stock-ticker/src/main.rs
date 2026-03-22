use std::{
    collections::HashMap, fmt, fs, net::{Ipv4Addr, UdpSocket}, time::{Duration, SystemTime, UNIX_EPOCH}
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
    /// Updates seq, drop count, and latency stats. Returns true if seq=0 (reset).
    pub fn update(&mut self, pairs: &HashMap<&str, &str>) -> color_eyre::Result<bool> {
        let seq: u32 = match pairs.get("seq").copied() {
            Some(s) => s.parse()?,
            _ => return Err(eyre!("missing seq")),
        };
        if seq == 0 {
            *self = Self::default();
            return Ok(true);
        }
        if seq > self.last_seq + 1 {
            self.drop_count += seq - (self.last_seq + 1);
        }
        self.last_seq = seq;
        self.total_count += 1;
        let sender_ts: u128 = match pairs.get("ts").copied() {
            Some(s) => s.parse()?,
            _ => return Err(eyre!("missing ts")),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        self.latency_sum += now.saturating_sub(sender_ts);
        self.latency_count += 1;
        if self.latency_count >= LATENCY_BATCH {
            self.latency_us = self.latency_sum / self.latency_count;
            self.latency_sum = 0;
            self.latency_count = 0;
        }
        Ok(false)
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

pub struct ItchTickListener {
    pub socket: UdpSocket,
    pub messages: Vec<ItchyMessage>,
    pub stats: Stats,
}

impl ItchTickListener {
    pub fn init() -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind("0.0.0.0:1234")?;
        socket.join_multicast_v4(
            &"239.255.0.1".parse::<Ipv4Addr>().unwrap(),
            &Ipv4Addr::UNSPECIFIED,
        )?;
        socket.set_nonblocking(true)?; // non-blocking so the TUI stays responsive

        Ok(Self {
            socket,
            messages: vec![],
            stats: Stats::default(),
        })
    }

    pub fn receive(&mut self) -> color_eyre::Result<Vec<ItchyMessage>> {
        let mut messages: Vec<ItchyMessage> = vec![];
        let mut buf = [0u8; 4096];

        // Drain incoming messages
        while let Ok((len, _)) = self.socket.recv_from(&mut buf) {
            let datagram = String::from_utf8_lossy(&buf[..len]).to_string();
            let pairs = ItchTickListener::parse_message(datagram.as_str());
            let itchy_message = match pairs.get("kind").copied() {
                Some("tick") => Trade::try_from_hashmap(&pairs).map(ItchyMessage::StockTick),
                _ => Err(eyre!("unknown kind")),
            }
            .wrap_err_with(|| format!("raw datagram: {datagram}"))?;

            messages.push(itchy_message);
            if self.stats.update(&pairs)? {
                messages.clear();
                messages.push(ItchyMessage::Reset);
                continue;
            }
        }
        Ok(messages)
    }

    fn parse_message(s: &str) -> HashMap<&str, &str> {
        s.split(';')
            .filter_map(|pair| pair.split_once('='))
            .collect()
    }
}

/// The main application which holds the state and logic of the application.
pub struct App {
    /// Is the application running?
    running: bool,
    itchy_listener: ItchTickListener,
    symbols: HashMap<String, f32>,
}

impl App {
    pub fn new() -> Result<Self, std::io::Error> {
        let itchy_listener = ItchTickListener::init()?;
        Ok(Self {
            running: true,
            itchy_listener,
            symbols: HashMap::<String, f32>::new(),
        })
    }

    /// Run the application's main loop.
    pub fn run(mut self, mut terminal: DefaultTerminal) -> color_eyre::Result<()> {
        self.running = true;
        // Ignore if there's no file here, ideally we would alert the user
        // but not going to bother here since that isn't part of the demo
        let _ = self.load_symbols();
        let _ = self.load_stats();
        while self.running {
            let messages = self.itchy_listener.receive()?;
            for msg in messages.iter() {
                match msg {
                    ItchyMessage::StockTick(trade) => {
                        self.symbols.insert(trade.symbol.to_owned(), trade.price);
                    }
                    ItchyMessage::Reset => {
                        self.symbols.clear();
                    }
                }
            }
            self.persist_symbols()?;
            self.persist_stats()?;
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
        let bytes = rmp_serde::to_vec(&self.itchy_listener.stats)?;
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
        self.itchy_listener.stats = rmp_serde::from_slice(&bytes)?;
        Ok(())
    }

    /// Renders the user interface.
    ///
    /// This is where you add new widgets. See the following resources for more information:
    ///
    /// - <https://docs.rs/ratatui/latest/ratatui/widgets/index.html>
    /// - <https://github.com/ratatui/ratatui/tree/main/ratatui-widgets/examples>
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
        let status_txt = self.itchy_listener.stats.to_string();
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
        self.running = false;
    }
}
