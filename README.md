# Kalshi Whale Detector 🐳

A real-time, event-driven distributed system for detecting anomalous institutional-sized trading activity ("whales") on the Kalshi prediction market.

This project architected a scalable pipeline to ingest live WebSocket feeds, process data asynchronously using message queues, and score trades in real-time using an unsupervised machine learning model (Isolation Forest) to detect statistical outliers.

---

## 🚀 Project Highlights

- Real-Time Intelligence: Latency from trade execution to Discord alert is sub-second.
- Distributed Architecture: Decoupled microservices design (Ingestor, Analyzer, Alerter) connected via RabbitMQ.
- Smart Context: Uses a custom "smart" model that normalizes trade size against dynamic market liquidity (Open Interest & 24h Volume) and categories (Sports vs. Economics).
- Robust Infrastructure: Optimized for constrained environments (4GB RAM) using chunked data loading, downsampling strategies, and Redis AOF persistence.
- Alert Routing: Automatically routes alerts to specific Discord channels based on market category (e.g., #sports, #politics).

---

## 🏗️ System Architecture

The system is composed of three independent Python microservices running in Docker containers on a Linux VPS:

- Ingestor (The "Ears"):
  - Connects to Kalshi's authenticated WebSocket API.
  - Subscribes to the global trade firehose.
  - Publishes raw trade JSON objects to the `raw_trades` queue in RabbitMQ.

- Analyzer (The "Brain"):
  - Consumes trades from RabbitMQ.
  - Fetches market context (Category, Volume, Open Interest) from Redis.
  - Builds a feature vector in real-time.
  - Scores the trade using a pre-trained Isolation Forest model (scikit-learn).
  - If the anomaly score crosses the threshold (e.g., < -0.7), it publishes an alert to the `alerts` queue.

- Alerter (The "Mouth"):
  - Consumes confirmed anomalies from RabbitMQ.
  - Formats a rich Discord Embed with market links and stats.
  - Routes the message to the correct Discord channel based on the market's category.

- Support Infrastructure:
  - RabbitMQ: Message broker for async communication.
  - Redis: High-performance in-memory cache for market baselines (persisted to disk).
  - DigitalOcean Droplet: Hosting environment.

---

## 🛠️ Tech Stack

- Language: Python 3.10+
- ML Frameworks: scikit-learn, pandas, numpy
- Infrastructure: Docker, RabbitMQ, Redis
- Cloud: DigitalOcean (Linux/Ubuntu)
- APIs: Kalshi Trade API v2, Discord Bot API
- Process Management: tmux, git

---

## 📦 Installation & Setup

### Prerequisites
- Python 3.10+
- Docker & Docker Compose (or Docker CLI)
- A Kalshi Account (API Key & Private Key)
- A Discord Bot Token

1. Clone the repository

```bash
git clone https://github.com/vnaut/kalshi-whale-detector.git
cd kalshi-whale-detector
```

2. Environment Setup

Create a `.env` file in the root directory to store your secrets. Do not commit this file.

```ini
# .env
KALSHI_API_KEY_ID=your_api_key_id
KALSHI_PRIVATE_KEY_PATH=private.key
DISCORD_BOT_TOKEN=your_discord_bot_token
REDIS_HOST=localhost
RABBITMQ_HOST=localhost
```

3. Start Infrastructure

Start the message broker and database containers.

```bash
docker run -d --name my-whale-broker -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --name my-whale-redis -p 6379:6379 -v redis_persistent_storage:/data redis redis-server --appendonly yes
```

4. Train the Model

Before running the real-time system, you must build the statistical baseline.

```bash
# This fetches 3 days of history, downsamples it, and trains the Isolation Forest.
python train_model.py
```

Artifacts created: `if_model.joblib`, `model_categories.json`

---

## 🏃‍♂️ Usage

To run the full system locally or on a server, start each service in its own terminal or process manager (tmux/systemd):

Terminal 1: The Alerter

```bash
python alerter.py
```

Terminal 2: The Analyzer

```bash
python analyzer.py
```

Terminal 3: The Ingestor

```bash
python ingestor.py
```

Note: For production deployment, these are run inside tmux sessions or as systemd services.

---

## 📊 Machine Learning Details

The model is an Isolation Forest trained on 7 core features:

- trade_count: Number of contracts in the specific trade.
- yes_price: The price the trade executed at.
- avg_trade_size: Baseline average trade size for that specific market ticker.
- size_z_score: Statistical deviation of this trade from the market mean.
- time_to_resolution: Hours remaining until the market closes.
- open_interest: Total liquidity in the market (Contextualizer).
- volume_24h: Recent market activity (Contextualizer).
- category: One-hot encoded categories (Sports, Politics, etc.) to learn sector-specific baselines.

Optimization: The training pipeline uses a memory-optimized chunk loader and downsampling (max 500k rows) to train successfully on hardware with limited RAM (4GB).

Thresholding: Trades that score beyond a configured anomaly threshold (for example, anomaly score < -0.7) are considered suspicious and are forwarded to the alert pipeline.

---

## 🔧 Operational Notes

- Redis is configured with AOF persistence to retain market baselines between restarts.
- The system is designed to operate reliably in constrained environments (4GB RAM) using chunked data loading and downsampling.
- Alerts are routed by market category—configure your Discord channels and bot permissions before enabling the Alerter.
- Use tmux or systemd to run services persistently on a droplet.

---

## 🛡️ Disclaimer

This software is for educational and research purposes only. It is not financial advice. Trading prediction markets involves risk. Use at your own risk.

---

## Contributing

Contributions, issues and feature requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

---
