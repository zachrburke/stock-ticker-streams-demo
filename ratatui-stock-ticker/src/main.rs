use std::{
    collections::HashMap,
    net::{Ipv4Addr, UdpSocket},
    time::Duration,
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
}

pub struct Trade {
    price: f32,
    symbol: String,
}

impl Trade {
    pub fn try_from_hashmap(map: HashMap<&str, &str>) -> color_eyre::Result<Trade> {
        let symbol = map.get("symbol").ok_or_else(|| eyre!("missing: symbol"))?;
        let price_raw = map.get("price").ok_or_else(|| eyre!("missing: price"))?;
        Ok(Trade {
            symbol: symbol.to_string(),
            price: price_raw.parse()?,
        })
    }
}

pub struct ItchTickListener {
    pub socket: UdpSocket,
    pub messages: Vec<ItchyMessage>,
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
                Some("tick") => Trade::try_from_hashmap(pairs).map(ItchyMessage::StockTick),
                _ => Err(eyre!("unknown kind")),
            }
            .wrap_err_with(|| format!("raw datagram: {datagram}"))?;

            messages.push(itchy_message);
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
        while self.running {
            let messages = self.itchy_listener.receive()?;
            for msg in messages.iter() {
                match msg {
                    ItchyMessage::StockTick(trade) => {
                        self.symbols.insert(trade.symbol.to_owned(), trade.price);
                    }
                }
            }
            terminal.draw(|frame| self.render(frame))?;
            self.handle_crossterm_events()?;
        }
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

        let title = Line::from("Ratatui Simple Template")
            .bold()
            .blue()
            .centered();
        let text = "Hello, Ratatui stock ticker!\n\n\
            Created using https://github.com/ratatui/templates\n\
            Press `Esc`, `Ctrl-C` or `q` to stop running.";
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
        if event::poll(Duration::from_millis(50))? {
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
