import requests
import pandas as pd
import numpy as np
import redis
import joblib
import json
import time
import logging
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Constants ---
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MODEL_FILE_PATH = 'if_model.joblib'
DAYS_OF_DATA_TO_PULL = 3
MODEL_CONTAMINATION = 0.005 
MAX_TRAIN_ROWS = 500000 # Downsampling limit to prevent hard OOM crashes

def get_all_series_categories() -> dict:
    logging.info("Fetching ALL series data from Kalshi to find true categories...")
    series_map = {}
    cursor = None
    try:
        while True:
            params = {'limit': 1000, 'cursor': cursor}
            response = requests.get(f"{KALSHI_API_BASE}/series", params={k: v for k, v in params.items() if v})
            response.raise_for_status()
            data = response.json()
            for series in data['series']:
                series_map[series['ticker']] = series.get('category', 'Other')
            cursor = data.get('cursor')
            if not cursor: break
        logging.info(f"Successfully loaded {len(series_map)} authoritative series categories.")
        return series_map
    except Exception as e:
        logging.error(f"Failed to fetch series: {e}")
        return {}

def get_all_markets(series_cat_map: dict) -> dict:
    logging.info("Fetching all markets from Kalshi...")
    markets = {}
    cursor = None
    try:
        while True:
            params = {'limit': 100, 'status': 'open,closed', 'cursor': cursor}
            response = requests.get(f"{KALSHI_API_BASE}/markets", params={k: v for k, v in params.items() if v})
            response.raise_for_status()
            data = response.json()
            for market in data['markets']:
                series_ticker = market.get('series_ticker')
                if not series_ticker and '-' in market['ticker']:
                     series_ticker = market['ticker'].split('-')[0]
                true_category = series_cat_map.get(series_ticker, 'Other')
                market['category'] = true_category
                markets[market['ticker']] = market
            cursor = data.get('cursor')
            if not cursor: break
        logging.info(f"Successfully fetched {len(markets)} markets with accurate categories.")
        return markets
    except Exception as e:
        logging.error(f"Error fetching markets: {e}")
        return {}
    
def get_trade_history(days: int) -> pd.DataFrame:
    logging.info(f"Fetching trades for last {days} days (Memory Optimized)...")
    df_chunks = []
    cursor = None
    min_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    keep_cols = ['market_ticker', 'count', 'yes_price', 'created_time']
    try:
        while True:
            params = {'limit': 1000, 'min_ts': min_ts, 'cursor': cursor}
            response = requests.get(f"{KALSHI_API_BASE}/markets/trades", params={k: v for k, v in params.items() if v})
            response.raise_for_status()
            data = response.json()
            if data.get('trades'):
                chunk = pd.DataFrame(data['trades'])
                if 'ticker' in chunk.columns and 'market_ticker' not in chunk.columns:
                     chunk = chunk.rename(columns={'ticker': 'market_ticker'})
                available_cols = [c for c in keep_cols if c in chunk.columns]
                chunk = chunk[available_cols]
                if 'count' in chunk.columns: chunk['count'] = chunk['count'].astype(np.int32)
                if 'yes_price' in chunk.columns: chunk['yes_price'] = chunk['yes_price'].astype(np.int8)
                df_chunks.append(chunk)
            cursor = data.get('cursor')
            if not cursor: break
        logging.info(f"Combining {len(df_chunks)} optimized chunks...")
        full_df = pd.concat(df_chunks, ignore_index=True)
        if 'market_ticker' in full_df.columns:
             full_df = full_df.rename(columns={'market_ticker': 'ticker'})
        logging.info(f"Successfully loaded {len(full_df)} trades.")
        return full_df
    except Exception as e:
        logging.error(f"Error fetching trade history: {e}")
        return pd.DataFrame()

def create_feature_matrix(trades_df: pd.DataFrame, market_data: dict) -> (pd.DataFrame, dict):
    if trades_df.empty: return pd.DataFrame(), {}
        
    logging.info(f"Starting feature engineering on {len(trades_df)} trades...")
    trades_df['created_time'] = pd.to_datetime(trades_df['created_time'], format='ISO8601')

    market_trade_baselines = trades_df.groupby('ticker')['count'].agg(
        avg_trade_size='mean',
        std_dev_trade_size='std'
    ).fillna(0)
    
    logging.info("Building master baseline DataFrame from market metadata...")
    market_baselines_df = pd.DataFrame.from_dict(market_data, orient='index')
    market_baselines_df = market_baselines_df.join(market_trade_baselines)

    defaults = {'url': 'https://kalshi.com/markets', 'title': 'Title Not Found', 'category': 'Other', 'open_interest': 0, 'volume_24h': 0}
    for col, val in defaults.items():
        if col not in market_baselines_df.columns: market_baselines_df[col] = val
        else: market_baselines_df[col] = market_baselines_df[col].fillna(val)

    # --- DOWNSAMPLING SAFETY NET ---
    if len(trades_df) > MAX_TRAIN_ROWS:
        logging.info(f"Downsampling training data to {MAX_TRAIN_ROWS} rows...")
        trades_df = trades_df.sample(n=MAX_TRAIN_ROWS, random_state=42)
    # ------------------------------

    columns_to_join = ['category', 'open_interest', 'volume_24h', 'close_time', 'avg_trade_size', 'std_dev_trade_size']
    market_baselines_to_join = market_baselines_df.reindex(columns=columns_to_join)
    trades_df = trades_df.join(market_baselines_to_join, on='ticker')
    
    # --- RESTORED: One-Hot Encoding ---
    logging.info("Applying One-Hot Encoding to categories...")
    trades_df = pd.get_dummies(trades_df, columns=['category'], prefix='category')

    trades_df['size_z_score'] = ((trades_df['count'] - trades_df['avg_trade_size']) / (trades_df['std_dev_trade_size'] + 1))
    trades_df['close_time'] = pd.to_datetime(trades_df['close_time'], errors='coerce')
    trades_df = trades_df.dropna(subset=['close_time', 'avg_trade_size'])
    trades_df['time_to_resolution_hrs'] = ((trades_df['close_time'] - trades_df['created_time']).dt.total_seconds() / 3600.0).clip(lower=0)
    
    # Define all features including categories
    feature_columns = ['count', 'yes_price', 'avg_trade_size', 'size_z_score', 'time_to_resolution_hrs', 'open_interest', 'volume_24h']
    category_columns = [col for col in trades_df.columns if col.startswith('category_')]
    feature_columns.extend(category_columns)
    
    logging.info(f"Model will be trained with {len(feature_columns)} features.")
    final_df = trades_df[feature_columns].dropna()
    
    # Save categories for analyzer
    try:
        with open('model_categories.json', 'w') as f:
            json.dump({'categories': category_columns}, f)
        logging.info(f"Saved {len(category_columns)} accurate categories to model_categories.json")
    except Exception as e:
        logging.error(f"Could not save model_categories.json: {e}")

    redis_columns = ['avg_trade_size', 'std_dev_trade_size', 'close_time', 'title', 'url', 'category', 'open_interest', 'volume_24h']
    final_baselines_for_redis = market_baselines_df.reindex(columns=redis_columns).fillna(0)
    for col, val in defaults.items():
         if isinstance(val, str):
             final_baselines_for_redis[col] = final_baselines_for_redis[col].replace(0, val)

    return final_df, final_baselines_for_redis.to_dict('index')

def store_baselines_in_redis(market_baselines_dict: dict):
    if not market_baselines_dict: return
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipeline = r.pipeline()
        for ticker, stats in market_baselines_dict.items():
            pipeline.set(f"baseline:{ticker}", json.dumps(stats))
        pipeline.execute()
        logging.info(f"Stored {len(market_baselines_dict)} market baselines in Redis.")
    except Exception as e:
        logging.error(f"Redis error: {e}")

def train_model(X_train: pd.DataFrame) -> IsolationForest:
    if X_train.empty: return None
    # n_jobs=1 keeps memory usage stable
    model = IsolationForest(n_estimators=100, contamination=MODEL_CONTAMINATION, random_state=42, n_jobs=1)
    logging.info("Fitting IsolationForest model...")
    model.fit(X_train)
    logging.info("Model fitting complete.")
    return model

def main():
    logging.info("--- Starting nightly model training job ---")
    start_time = time.time()
    
    series_cat_map = get_all_series_categories()
    if not series_cat_map: return

    market_data = get_all_markets(series_cat_map)
    if not market_data: return

    trades_df = get_trade_history(days=DAYS_OF_DATA_TO_PULL)
    if trades_df.empty: return

    X_train, market_baselines = create_feature_matrix(trades_df, market_data)
    store_baselines_in_redis(market_baselines)
    model = train_model(X_train)
    
    if model:
        joblib.dump(model, MODEL_FILE_PATH)
        logging.info(f"Successfully trained and saved model to {MODEL_FILE_PATH}")

    logging.info(f"--- Nightly job finished in {time.time() - start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()