import pika
import redis
import joblib
import json
import logging
import pandas as pd
import warnings
from datetime import datetime, timezone

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
warnings.filterwarnings("ignore", category=UserWarning, message="X does not have valid feature names")

RABBITMQ_HOST = 'localhost'
INPUT_QUEUE = 'raw_trades'
OUTPUT_QUEUE = 'alerts'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MODEL_FILE_PATH = 'if_model.joblib'
MODEL_CAT_FILE = 'model_categories.json'
ANOMALY_SCORE_THRESHOLD = -0.7

def load_artifacts():
    """Loads the trained model, Redis client, and model categories."""
    logging.info(f"Loading model from {MODEL_FILE_PATH}...")
    try:
        model = joblib.load(MODEL_FILE_PATH)
        logging.info("Model loaded successfully.")
    except Exception as e:
        logging.error(f"FATAL: Error loading model: {e}")
        return None, None, None

    logging.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        logging.info("Redis connected successfully.")
    except Exception as e:
        logging.error(f"FATAL: Redis connection failed: {e}")
        return None, None, None
        
    # --- RESTORED: Loading model_categories.json ---
    logging.info(f"Loading model categories from {MODEL_CAT_FILE}...")
    try:
        with open(MODEL_CAT_FILE, 'r') as f:
            model_categories = json.load(f)['categories']
        logging.info(f"Loaded {len(model_categories)} category features.")
    except Exception as e:
        logging.error(f"FATAL: Failed to load {MODEL_CAT_FILE}. Run train_model.py first! {e}")
        return None, None, None
        
    return model, redis_client, model_categories

def main():
    model, redis_client, model_categories = load_artifacts()
    if not all([model, redis_client, model_categories]): return

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=INPUT_QUEUE, durable=True)
        channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)
        logging.info(f"RabbitMQ connected. Waiting for trades from '{INPUT_QUEUE}'...")
    except Exception as e:
        logging.error(f"FATAL: RabbitMQ connection failed: {e}")
        return

    def callback(ch, method, properties, body):
        try:
            trade = json.loads(body)
            ticker = trade.get('market_ticker')
            if not ticker:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            baseline_data = redis_client.get(f"baseline:{ticker}")
            if not baseline_data:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            baselines = json.loads(baseline_data)

            # Use authoritative category from Redis
            category = baselines.get('category', 'Other')

            count = trade.get('count')
            yes_price = trade.get('yes_price')
            trade_time_ts = trade.get('ts')
            avg_trade_size = baselines.get('avg_trade_size')
            std_dev_trade_size = baselines.get('std_dev_trade_size')
            close_time_str = baselines.get('close_time')
            open_interest = baselines.get('open_interest', 0)
            volume_24h = baselines.get('volume_24h', 0)

            if any(v is None for v in [count, yes_price, trade_time_ts, avg_trade_size, std_dev_trade_size, close_time_str]):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            size_z_score = (count - avg_trade_size) / (std_dev_trade_size + 1)
            trade_time_dt = datetime.fromtimestamp(trade_time_ts, tz=timezone.utc)
            
            # Robust timestamp parsing
            close_time_dt = pd.to_datetime(close_time_str).to_pydatetime()
            if close_time_dt.tzinfo is None: close_time_dt = close_time_dt.replace(tzinfo=timezone.utc)
            time_to_resolution_hrs = max(0, (close_time_dt - trade_time_dt).total_seconds() / 3600.0)

            # --- RESTORED: One-Hot Encoding Logic ---
            # Create a blank template of 0s for all known categories
            category_features = {col: 0 for col in model_categories}
            
            # Set the current category to 1
            trade_category_col = f"category_{category}"
            if trade_category_col in category_features:
                category_features[trade_category_col] = 1
            
            # Build full vector: 7 numeric stats + all category columns
            feature_vector = [
                count, yes_price, avg_trade_size, size_z_score, time_to_resolution_hrs,
                open_interest, volume_24h
            ]
            feature_vector.extend([category_features[col] for col in model_categories])

            score = model.score_samples([feature_vector])[0]

            if score < ANOMALY_SCORE_THRESHOLD:
                logging.warning(f"--- WHALE DETECTED! Score: {score:.4f}, Ticker: {ticker}, Size: {count} ---")
                taker_side = trade.get('taker_side')
                price_paid_cents = trade.get('no_price') if taker_side == 'no' else trade.get('yes_price')

                alert_msg = {
                    "ticker": ticker, "trade_size": count, "price_paid": price_paid_cents,
                    "taker_side": taker_side, "anomaly_score": score,
                    "trade_time": trade_time_dt.isoformat(), "market_close_time": close_time_dt.isoformat(),
                    "title": baselines.get('title', 'Title Not Found'),
                    "url": baselines.get('url', 'https://kalshi.com/markets'),
                    "category": category
                }
                channel.basic_publish(exchange='', routing_key=OUTPUT_QUEUE, body=json.dumps(alert_msg), properties=pika.BasicProperties(delivery_mode=2))
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Shutting down analyzer...")
        channel.stop_consuming()
    connection.close()

if __name__ == "__main__":
    main()