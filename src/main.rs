use std::collections::HashMap;
use std::net::{UdpSocket, SocketAddr};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

// ============================================
// HARDCODED STREAMING SOURCE CONFIGURATION
// ============================================
const STREAMING_SOURCE_IP: &str = "34.127.57.0";   // GCP streaming service IP
const STREAMING_SOURCE_PORT: u16 = 8888;           // GCP streaming service port

// ============================================
// SIGNAL OUTPUT UDP CONFIGURATION
// ============================================
const SIGNAL_OUTPUT_PORT: u16 = 9999;              // Port to stream signals on
const SIGNAL_OUTPUT_BIND_IP: &str = "0.0.0.0";     // IP to bind signal output to

// Configuration for the trading strategy
#[derive(Clone, Debug)]
pub struct StrategyConfig {
    pub imbalance_threshold: f64,
    pub min_volume_threshold: f64,
    pub lookback_periods: usize,
    pub signal_cooldown_ms: u64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            imbalance_threshold: 0.6,
            min_volume_threshold: 10.0,
            lookback_periods: 5,
            signal_cooldown_ms: 100,
        }
    }
}

// Subscription request structure (matches Python client)
#[derive(Debug, Serialize)]
pub struct SubscriptionRequest {
    pub action: String,  // "start" or "stop"
    pub symbol: String,
}

// Market data structure from UDP feed - UPDATED to match Python structure
#[derive(Debug, Clone, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub price: f64,
    pub volume: f64,
    pub bid: f64,
    pub ask: f64,
    pub timestamp: u64,
    pub exchange: String,
}

// Trading signal output
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum Signal {
    Buy {
        symbol: String,
        timestamp: u64,
        confidence: f64,
        imbalance_ratio: f64,
        mid_price: f64,
    },
    Sell {
        symbol: String,
        timestamp: u64,
        confidence: f64,
        imbalance_ratio: f64,
        mid_price: f64,
    },
}
// Order book imbalance metrics
#[derive(Debug, Clone)]
struct ImbalanceMetrics {
    bid_volume: f64,
    ask_volume: f64,
    total_volume: f64,
    imbalance_ratio: f64,
    timestamp: u64,
}

impl ImbalanceMetrics {
    fn new(bid: f64, ask: f64, volume: f64, timestamp: u64) -> Self {
        // Use bid/ask prices and volume to estimate imbalance
        let spread = ask - bid;
        let mid_price = (bid + ask) / 2.0;
        
        // Simple imbalance calculation based on spread and volume
        let total_volume = volume;
        let imbalance_ratio = if spread > 0.0 {
            // Wider spread might indicate more ask pressure (negative imbalance)
            // This is a simplified calculation - you might want to adjust this logic
            (bid - mid_price) / (spread / 2.0)
        } else {
            0.0
        };

        Self {
            bid_volume: volume / 2.0,  // Simplified assumption
            ask_volume: volume / 2.0,  // Simplified assumption
            total_volume,
            imbalance_ratio,
            timestamp,
        }
    }
}

// Main strategy implementation
pub struct OrderBookImbalanceStrategy {
    config: StrategyConfig,
    metrics_history: HashMap<String, Vec<ImbalanceMetrics>>,
    last_signal_time: HashMap<String, Instant>,
    signal_sender: crossbeam_channel::Sender<Signal>,
}

impl OrderBookImbalanceStrategy {
    pub fn new(config: StrategyConfig) -> (Self, crossbeam_channel::Receiver<Signal>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        
        let strategy = Self {
            config,
            metrics_history: HashMap::new(),
            last_signal_time: HashMap::new(),
            signal_sender: tx,
        };
        
        (strategy, rx)
    }

    pub fn process_market_data(&mut self, data: MarketData) {
        let metrics = ImbalanceMetrics::new(
            data.bid,
            data.ask,
            data.volume,
            data.timestamp,
        );
        
        // Debug output
        println!("📊 {}: ${:.4} | Vol: {:.2} | Bid: ${:.4} | Ask: ${:.4}", 
                 data.symbol, data.price, data.volume, data.bid, data.ask);
        
        // Store metrics history
        let history = self.metrics_history
            .entry(data.symbol.clone())
            .or_insert_with(Vec::new);
        
        history.push(metrics.clone());
        
        // Keep only recent history
        if history.len() > self.config.lookback_periods {
            history.remove(0);
        }

        // Generate signal if conditions are met
        if let Some(signal) = self.evaluate_signal(&data.symbol, &data, &metrics) {
            if self.should_emit_signal(&data.symbol) {
                self.last_signal_time.insert(data.symbol.clone(), Instant::now());
                let _ = self.signal_sender.send(signal);
            }
        }
    }

    fn evaluate_signal(&self, symbol: &str, data: &MarketData, current_metrics: &ImbalanceMetrics) -> Option<Signal> {
        if current_metrics.total_volume < self.config.min_volume_threshold {
            return None;
        }

        let history = self.metrics_history.get(symbol)?;
        if history.len() < 2 {
            return None;
        }

        let avg_imbalance = history.iter()
            .map(|m| m.imbalance_ratio)
            .sum::<f64>() / history.len() as f64;

        let imbalance_consistency = self.calculate_consistency(history);
        let magnitude_factor = current_metrics.imbalance_ratio.abs();
        let confidence = (imbalance_consistency * magnitude_factor).min(1.0);

        let mid_price = (data.bid + data.ask) / 2.0;

        if avg_imbalance > self.config.imbalance_threshold {
            Some(Signal::Buy {
                symbol: symbol.to_string(),
                timestamp: data.timestamp,
                confidence,
                imbalance_ratio: avg_imbalance,
                mid_price,
            })
        } else if avg_imbalance < -self.config.imbalance_threshold {
            Some(Signal::Sell {
                symbol: symbol.to_string(),
                timestamp: data.timestamp,
                confidence,
                imbalance_ratio: avg_imbalance,
                mid_price,
            })
        } else {
            None
        }
    }

    fn calculate_consistency(&self, history: &[ImbalanceMetrics]) -> f64 {
        if history.len() < 2 {
            return 0.0;
        }

        let ratios: Vec<f64> = history.iter().map(|m| m.imbalance_ratio).collect();
        let mean = ratios.iter().sum::<f64>() / ratios.len() as f64;
        
        let variance = ratios.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / ratios.len() as f64;
        
        let std_dev = variance.sqrt();
        
        if mean.abs() < f64::EPSILON {
            return 0.0;
        }
        
        let cv = std_dev / mean.abs();
        (1.0 / (1.0 + cv)).min(1.0)
    }

    fn should_emit_signal(&self, symbol: &str) -> bool {
        if let Some(last_time) = self.last_signal_time.get(symbol) {
            last_time.elapsed().as_millis() as u64 >= self.config.signal_cooldown_ms
        } else {
            true
        }
    }
}

// UPDATED: UDP client that subscribes to streaming service
pub struct UdpMarketDataConsumer {
    socket: UdpSocket,
    strategy: Arc<Mutex<OrderBookImbalanceStrategy>>,
    server_addr: SocketAddr,
    subscribed_symbols: Vec<String>,
}

impl UdpMarketDataConsumer {
    pub fn new_with_hardcoded_config(strategy: OrderBookImbalanceStrategy) -> Result<Self, Box<dyn std::error::Error>> {
        // Create client socket (bind to any available port)
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        let local_addr = socket.local_addr()?;
        
        println!("✓ Client socket bound to: {}", local_addr);
        
        // Server address to connect to
        let server_addr: SocketAddr = format!("{}:{}", STREAMING_SOURCE_IP, STREAMING_SOURCE_PORT).parse()?;
        println!("✓ Streaming server address: {}", server_addr);
        
        socket.set_read_timeout(Some(Duration::from_millis(1000)))?;
        
        Ok(Self {
            socket,
            strategy: Arc::new(Mutex::new(strategy)),
            server_addr,
            subscribed_symbols: Vec::new(),
        })
    }

    // Subscribe to a cryptocurrency symbol
    pub fn subscribe(&mut self, symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
        let request = SubscriptionRequest {
            action: "start".to_string(),
            symbol: symbol.to_uppercase(),
        };
        
        let json_data = serde_json::to_string(&request)?;
        
        match self.socket.send_to(json_data.as_bytes(), &self.server_addr) {
            Ok(_) => {
                println!("✓ Subscribed to {}", symbol.to_uppercase());
                if !self.subscribed_symbols.contains(&symbol.to_uppercase()) {
                    self.subscribed_symbols.push(symbol.to_uppercase());
                }
                Ok(())
            }
            Err(e) => {
                println!("❌ Failed to subscribe to {}: {}", symbol, e);
                Err(Box::new(e))
            }
        }
    }

    // Unsubscribe from a cryptocurrency symbol
    pub fn unsubscribe(&mut self, symbol: &str) -> Result<(), Box<dyn std::error::Error>> {
        let request = SubscriptionRequest {
            action: "stop".to_string(),
            symbol: symbol.to_uppercase(),
        };
        
        let json_data = serde_json::to_string(&request)?;
        
        match self.socket.send_to(json_data.as_bytes(), &self.server_addr) {
            Ok(_) => {
                println!("✓ Unsubscribed from {}", symbol.to_uppercase());
                self.subscribed_symbols.retain(|s| s != &symbol.to_uppercase());
                Ok(())
            }
            Err(e) => {
                println!("❌ Failed to unsubscribe from {}: {}", symbol, e);
                Err(Box::new(e))
            }
        }
    }

    pub fn start_consuming(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Starting UDP consumption loop...");
        println!("📡 Listening for data from server: {}", self.server_addr);
        
        let mut buffer = [0u8; 4096];
        
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((size, addr)) => {
                    let data_str = String::from_utf8_lossy(&buffer[..size]);
                    println!("📦 Received {} bytes from {}", size, addr);
                    
                    // Parse JSON market data
                    match serde_json::from_str::<MarketData>(&data_str) {
                        Ok(market_data) => {
                            if let Ok(mut strategy) = self.strategy.lock() {
                                strategy.process_market_data(market_data);
                            }
                        }
                        Err(e) => {
                            println!("❌ Failed to parse market data: {}", e);
                            println!("Raw data: {}", data_str);
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                    // Timeout - continue loop
                    continue;
                }
                Err(e) => {
                    eprintln!("❌ UDP receive error: {}", e);
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    // Clean shutdown
    pub fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🛑 Shutting down client...");
        
        // Unsubscribe from all symbols
        for symbol in self.subscribed_symbols.clone() {
            let _ = self.unsubscribe(&symbol);
        }
        
        Ok(())
    }
}

// NEW: UDP Signal Broadcaster - streams signals to UDP clients
pub struct UdpSignalBroadcaster {
    socket: Arc<UdpSocket>,
    clients: Arc<Mutex<Vec<SocketAddr>>>,
}

impl UdpSignalBroadcaster {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let bind_addr = format!("{}:{}", SIGNAL_OUTPUT_BIND_IP, SIGNAL_OUTPUT_PORT);
        let socket = UdpSocket::bind(&bind_addr)?;
        
        println!("🎯 Signal UDP broadcaster bound to: {}", bind_addr);
        println!("📡 Clients can connect by sending any message to this address");
        
        // Set non-blocking for client registration checks
        socket.set_nonblocking(true)?;
        
        Ok(Self {
            socket: Arc::new(socket),
            clients: Arc::new(Mutex::new(Vec::new())),
        })
    }
    
    pub fn start_client_listener(&self) -> Result<(), Box<dyn std::error::Error>> {
        let socket_clone = self.socket.clone();
        let clients_clone = self.clients.clone();
        
        thread::spawn(move || {
            let mut buffer = [0u8; 1024];
            println!("👂 Client registration listener started");
            
            loop {
                match socket_clone.recv_from(&mut buffer) {
                    Ok((size, addr)) => {
                        let message = String::from_utf8_lossy(&buffer[..size]);
                        println!("📞 Client registration from {}: {}", addr, message.trim());
                        
                        if let Ok(mut clients) = clients_clone.lock() {
                            if !clients.contains(&addr) {
                                clients.push(addr);
                                println!("✅ Added client: {} (total: {})", addr, clients.len());
                                
                                // Send acknowledgment
                                let ack = "CONNECTED";
                                let _ = socket_clone.send_to(ack.as_bytes(), addr);
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No data available, continue
                        thread::sleep(Duration::from_millis(10));
                        continue;
                    }
                    Err(e) => {
                        eprintln!("❌ Client listener error: {}", e);
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });
        
        Ok(())
    }
    
    pub fn broadcast_signal(&self, signal: &Signal) -> Result<(), Box<dyn std::error::Error>> {
        let json_signal = serde_json::to_string(signal)?;
        
        if let Ok(mut clients) = self.clients.lock() {
            if clients.is_empty() {
                // Still log to console if no UDP clients
                println!("🚨 SIGNAL (no UDP clients): {}", json_signal);
                return Ok(());
            }
            
            println!("📡 Broadcasting signal to {} clients: {}", clients.len(), json_signal);
            
            let mut failed_clients = Vec::new();
            
            for (i, &client_addr) in clients.iter().enumerate() {
                match self.socket.send_to(json_signal.as_bytes(), client_addr) {
                    Ok(_) => {
                        println!("  ✅ Sent to client {}: {}", i + 1, client_addr);
                    }
                    Err(e) => {
                        println!("  ❌ Failed to send to {}: {}", client_addr, e);
                        failed_clients.push(client_addr);
                    }
                }
            }
            
            // Remove failed clients (they may have disconnected)
            for failed_addr in failed_clients {
                clients.retain(|&addr| addr != failed_addr);
                println!("🗑️  Removed disconnected client: {}", failed_addr);
            }
        }
        
        Ok(())
    }
    
    pub fn get_client_count(&self) -> usize {
        self.clients.lock().map(|c| c.len()).unwrap_or(0)
    }
}

// UPDATED: Signal output handler with UDP broadcasting
pub struct SignalOutput {
    receiver: crossbeam_channel::Receiver<Signal>,
    udp_broadcaster: UdpSignalBroadcaster,
}

impl SignalOutput {
    pub fn new(receiver: crossbeam_channel::Receiver<Signal>) -> Result<Self, Box<dyn std::error::Error>> {
        let udp_broadcaster = UdpSignalBroadcaster::new()?;
        
        Ok(Self { 
            receiver,
            udp_broadcaster,
        })
    }

    pub fn start_output_stream(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Start client listener
        self.udp_broadcaster.start_client_listener()?;
        
        // Start signal broadcasting thread
        let receiver = self.receiver.clone();
        let broadcaster = &self.udp_broadcaster;
        
        // We need to clone the broadcaster for the thread
        let socket = broadcaster.socket.clone();
        let clients = broadcaster.clients.clone();
        
        thread::spawn(move || {
            println!("📊 Signal output thread started");
            loop {
                match receiver.recv() {
                    Ok(signal) => {
                        // Serialize signal to JSON
                        match serde_json::to_string(&signal) {
                            Ok(json_signal) => {
                                // Broadcast via UDP
                                if let Ok(mut client_list) = clients.lock() {
                                    if client_list.is_empty() {
                                        // Still log to console if no UDP clients
                                        println!("🚨 SIGNAL (no UDP clients): {}", json_signal);
                                    } else {
                                        println!("📡 Broadcasting signal to {} clients: {}", client_list.len(), json_signal);
                                        
                                        let mut failed_clients = Vec::new();
                                        
                                        for (i, &client_addr) in client_list.iter().enumerate() {
                                            match socket.send_to(json_signal.as_bytes(), client_addr) {
                                                Ok(_) => {
                                                    println!("  ✅ Sent to client {}: {}", i + 1, client_addr);
                                                }
                                                Err(e) => {
                                                    println!("  ❌ Failed to send to {}: {}", client_addr, e);
                                                    failed_clients.push(client_addr);
                                                }
                                            }
                                        }
                                        
                                        // Remove failed clients
                                        for failed_addr in failed_clients {
                                            client_list.retain(|&addr| addr != failed_addr);
                                            println!("🗑️  Removed disconnected client: {}", failed_addr);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("❌ Failed to serialize signal: {}", e);
                            }
                        }
                    }
                    Err(_) => {
                        println!("📊 Signal output thread: Channel closed");
                        break;
                    }
                }
            }
        });
        
        Ok(())
    }
    
    pub fn get_client_count(&self) -> usize {
        self.udp_broadcaster.get_client_count()
    }
}

// Main application
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 Starting Order-Book Imbalance Strategy...");
    println!("🌐 Remote streaming server: {}:{}", STREAMING_SOURCE_IP, STREAMING_SOURCE_PORT);
    println!("📡 Signal UDP output: {}:{}", SIGNAL_OUTPUT_BIND_IP, SIGNAL_OUTPUT_PORT);
    
    // Configuration
    let strategy_config = StrategyConfig::default();
    
    // Create strategy and signal receiver
    let (strategy, signal_receiver) = OrderBookImbalanceStrategy::new(strategy_config);
    
    // Create consumer with hardcoded configuration
    let mut consumer = UdpMarketDataConsumer::new_with_hardcoded_config(strategy)?;
    
    // Start signal output stream with UDP broadcasting in background thread
    let signal_output = SignalOutput::new(signal_receiver)?;
    signal_output.start_output_stream()?;
    
    // Subscribe to some cryptocurrencies (like the Python example)
    let symbols = ["BTC", "ETH", "ADA", "SOL"];
    for symbol in &symbols {
        consumer.subscribe(symbol)?;
        thread::sleep(Duration::from_millis(100)); // Small delay between subscriptions
    }
    
    println!("✅ Strategy initialized successfully!");
    println!("📡 Listening for data from {} symbols...", symbols.len());
    println!("🎯 Broadcasting buy/sell signals via UDP on port {}", SIGNAL_OUTPUT_PORT);
    println!("📞 UDP clients can connect by sending any message to {}:{}", SIGNAL_OUTPUT_BIND_IP, SIGNAL_OUTPUT_PORT);
    println!("Press Ctrl+C to stop");
    
    // Print client count periodically
    let client_count_monitor = signal_output.udp_broadcaster.clients.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(30));
            if let Ok(clients) = client_count_monitor.lock() {
                let count = clients.len();
                if count > 0 {
                    println!("👥 Connected UDP clients: {}", count);
                }
            }
        }
    });
    
    // Set up Ctrl+C handler
    let consumer = Arc::new(Mutex::new(consumer));
    let consumer_clone = consumer.clone();
    
    ctrlc::set_handler(move || {
        println!("\n👋 Received Ctrl+C, shutting down...");
        if let Ok(mut consumer) = consumer_clone.lock() {
            let _ = consumer.shutdown();
        }
        std::process::exit(0);
    })?;
    
    // Start consuming market data (this blocks)
    if let Ok(mut consumer) = consumer.lock() {
        consumer.start_consuming()?;
    }
    
    Ok(())
}