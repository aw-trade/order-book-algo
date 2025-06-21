use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};

// Configuration for the trading strategy
#[derive(Clone, Debug)]
pub struct StrategyConfig {
    pub imbalance_threshold: f64,      // Minimum imbalance ratio to trigger signal
    pub min_volume_threshold: f64,     // Minimum total volume to consider
    pub lookback_periods: usize,       // Number of periods for moving average
    pub signal_cooldown_ms: u64,       // Minimum time between signals
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            imbalance_threshold: 0.6,      // 60% imbalance
            min_volume_threshold: 10.0,
            lookback_periods: 5,
            signal_cooldown_ms: 100,      // 1 second cooldown
        }
    }
}

// Market data structure from UDP feed
#[derive(Debug, Clone, Deserialize)]
pub struct MarketData {
    pub symbol: String,
    pub timestamp: u64,
    pub bid_price: f64,
    pub bid_volume: f64,
    pub ask_price: f64,
    pub ask_volume: f64,
    pub mid_price: f64,
}

// Trading signal output
#[derive(Debug, Clone, Serialize)]
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
    imbalance_ratio: f64, // (bid_volume - ask_volume) / total_volume
    timestamp: u64,
}

impl ImbalanceMetrics {
    fn new(bid_volume: f64, ask_volume: f64, timestamp: u64) -> Self {
        let total_volume = bid_volume + ask_volume;
        let imbalance_ratio = if total_volume > 0.0 {
            (bid_volume - ask_volume) / total_volume
        } else {
            0.0
        };

        Self {
            bid_volume,
            ask_volume,
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
            data.bid_volume,
            data.ask_volume,
            data.timestamp,
        );
        println!("Bid Volume: {}", data.bid_volume);
        println!("Ask Volume: {}", data.bid_volume);
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
        // Check minimum volume threshold
        if current_metrics.total_volume < self.config.min_volume_threshold {
            return None;
        }

        // Get historical metrics for smoothing
        let history = self.metrics_history.get(symbol)?;
        if history.len() < 2 {
            return None;
        }

        // Calculate moving average of imbalance ratio
        let avg_imbalance = history.iter()
            .map(|m| m.imbalance_ratio)
            .sum::<f64>() / history.len() as f64;

        // Calculate confidence based on consistency and magnitude
        let imbalance_consistency = self.calculate_consistency(history);
        let magnitude_factor = current_metrics.imbalance_ratio.abs();
        let confidence = (imbalance_consistency * magnitude_factor).min(1.0);

        // Generate signals based on imbalance direction and threshold
        if avg_imbalance > self.config.imbalance_threshold {
            // Strong bid pressure - Buy signal
            Some(Signal::Buy {
                symbol: symbol.to_string(),
                timestamp: data.timestamp,
                confidence,
                imbalance_ratio: avg_imbalance,
                mid_price: data.mid_price,
            })
        } else if avg_imbalance < -self.config.imbalance_threshold {
            // Strong ask pressure - Sell signal
            Some(Signal::Sell {
                symbol: symbol.to_string(),
                timestamp: data.timestamp,
                confidence,
                imbalance_ratio: avg_imbalance,
                mid_price: data.mid_price,
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
        
        // Calculate coefficient of variation (lower is more consistent)
        let variance = ratios.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / ratios.len() as f64;
        
        let std_dev = variance.sqrt();
        
        if mean.abs() < f64::EPSILON {
            return 0.0;
        }
        
        // Return consistency factor (inverse of coefficient of variation)
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

// UDP market data consumer
pub struct UdpMarketDataConsumer {
    socket: UdpSocket,
    strategy: Arc<Mutex<OrderBookImbalanceStrategy>>,
}

impl UdpMarketDataConsumer {
    pub fn new(bind_addr: &str, strategy: OrderBookImbalanceStrategy) -> Result<Self, Box<dyn std::error::Error>> {
        let socket = Self::bind_with_retry(bind_addr)?;
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;
        
        Ok(Self {
            socket,
            strategy: Arc::new(Mutex::new(strategy)),
        })
    }

    fn bind_with_retry(preferred_addr: &str) -> Result<UdpSocket, Box<dyn std::error::Error>> {
        // Try the preferred address first
        match UdpSocket::bind(preferred_addr) {
            Ok(socket) => {
                println!("Successfully bound to: {}", preferred_addr);
                return Ok(socket);
            }
            Err(e) => {
                println!("Failed to bind to {}: {}", preferred_addr, e);
            }
        }

        // If preferred address fails, try alternative ports
        let base_ip = if preferred_addr.contains(':') {
            preferred_addr.split(':').next().unwrap_or("127.0.0.1")
        } else {
            "127.0.0.1"
        };

        for port in 8081..8100 {
            let addr = format!("{}:{}", base_ip, port);
            match UdpSocket::bind(&addr) {
                Ok(socket) => {
                    println!("Successfully bound to alternative address: {}", addr);
                    return Ok(socket);
                }
                Err(_) => continue,
            }
        }

        // If all else fails, let the OS choose a port
        let auto_addr = format!("{}:0", base_ip);
        match UdpSocket::bind(&auto_addr) {
            Ok(socket) => {
                let local_addr = socket.local_addr()?;
                println!("Successfully bound to OS-assigned address: {}", local_addr);
                Ok(socket)
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    pub fn start_consuming(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = [0u8; 1024];
        
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((size, _addr)) => {
                    let data_str = String::from_utf8_lossy(&buffer[..size]);
                    
                    // Parse JSON market data
                    match serde_json::from_str::<MarketData>(&data_str) {
                        Ok(market_data) => {
                            if let Ok(mut strategy) = self.strategy.lock() {
                                strategy.process_market_data(market_data);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to parse market data: {}", e);
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Timeout - continue loop
                    continue;
                }
                Err(e) => {
                    eprintln!("UDP receive error: {}", e);
                }
            }
        }
    }
}

// Signal output handler
pub struct SignalOutput {
    receiver: crossbeam_channel::Receiver<Signal>,
}

impl SignalOutput {
    pub fn new(receiver: crossbeam_channel::Receiver<Signal>) -> Self {
        Self { receiver }
    }

    pub fn start_output_stream(&self) {
        thread::spawn({
            let receiver = self.receiver.clone();
            move || {
                loop {
                    match receiver.recv() {
                        Ok(signal) => {
                            // Output signal as JSON to stdout
                            match serde_json::to_string(&signal) {
                                Ok(json) => println!("{}", json),
                                Err(e) => eprintln!("Failed to serialize signal: {}", e),
                            }
                        }
                        Err(_) => {
                            // Channel closed
                            break;
                        }
                    }
                }
            }
        });
    }
}

// Main application with command line argument support
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    // Parse command line arguments
    let bind_addr = if args.len() > 1 {
        &args[1]
    } else {
        "127.0.0.1:8080" // Default address
    };

    // Configuration
    let config = StrategyConfig::default();

    println!("Starting Order-Book Imbalance Strategy...");
    
    // Create strategy and signal receiver
    let (strategy, signal_receiver) = OrderBookImbalanceStrategy::new(config);
    
    // Create UDP consumer with automatic port selection
    let consumer = UdpMarketDataConsumer::new(bind_addr, strategy)?;
    
    // Start signal output stream
    let signal_output = SignalOutput::new(signal_receiver);
    signal_output.start_output_stream();
    
    println!("Strategy initialized successfully!");
    println!("Outputting buy/sell signals to stdout...");
    println!("Send market data as JSON to the UDP endpoint.");
    
    // Start consuming market data (blocking)
    consumer.start_consuming()?;
    println!("Started Consumption");
    
    Ok(())
}

// Example market data generator for testing
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn generate_test_market_data(symbol: &str, bid_vol: f64, ask_vol: f64) -> MarketData {
        MarketData {
            symbol: symbol.to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            bid_price: 100.0,
            bid_volume: bid_vol,
            ask_price: 100.1,
            ask_volume: ask_vol,
            mid_price: 100.05,
        }
    }

    #[test]
    fn test_imbalance_calculation() {
        let config = StrategyConfig::default();
        let (mut strategy, _rx) = OrderBookImbalanceStrategy::new(config);

        // Test strong bid imbalance (should generate buy signal)
        let data = generate_test_market_data("BTCUSD", 8000.0, 2000.0);
        strategy.process_market_data(data.clone());
        
        // Add more data to build history
        for _ in 0..5 {
            let data = generate_test_market_data("BTCUSD", 7500.0, 2500.0);
            strategy.process_market_data(data);
            thread::sleep(Duration::from_millis(10));
        }
    }

    // Example UDP data sender for testing
    pub fn send_test_data(target_port: Option<u16>) -> Result<(), Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind("127.0.0.1:0")?;
        let port = target_port.unwrap_or(8080);
        let target_addr = format!("127.0.0.1:{}", port);

        println!("Sending test data to: {}", target_addr);

        loop {
            // Simulate varying imbalance conditions
            let bid_vol = 5000.0 + (rand::random::<f64>() - 0.5) * 4000.0;
            let ask_vol = 3000.0 + (rand::random::<f64>() - 0.5) * 2000.0;
            
            let data = generate_test_market_data("BTCUSD", bid_vol, ask_vol);
            let json = serde_json::to_string(&data)?;
            
            match socket.send_to(json.as_bytes(), &target_addr) {
                Ok(_) => println!("Sent: bid={:.0}, ask={:.0}, imbalance={:.2}", 
                    bid_vol, ask_vol, (bid_vol - ask_vol) / (bid_vol + ask_vol)),
                Err(e) => eprintln!("Failed to send data: {}", e),
            }
            
            thread::sleep(Duration::from_millis(500));
        }
    }
}
