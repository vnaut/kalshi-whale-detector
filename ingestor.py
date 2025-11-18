import os
from dotenv import load_dotenv
import websockets
import asyncio
import json
import pika
import time
import base64
import logging
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend

load_dotenv()

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")  
PRIVATE_KEY_PATH = "private.key"                

KALSHI_WSS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'raw_trades' # The queue we will send trades to

def load_private_key(path):
    """Loads a PEM private key from a file."""
    logging.info(f"Loading private key from {path}...")
    try:
        with open(path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None, # Assuming your key is not password-protected
                backend=default_backend()
            )
        logging.info("Private key loaded successfully.")
        return private_key
    except Exception as e:
        logging.error(f"Failed to load private key: {e}")
        logging.error("Please make sure 'private.key' is in the same folder.")
        return None

def create_auth_headers(private_key, api_key_id):
    """Creates the necessary headers for WebSocket authentication."""
    timestamp = str(int(time.time() * 1000)) # Milliseconds
    
    # This is the specific string Kalshi requires for WebSocket auth
    path_to_sign = "/trade-api/ws/v2"
    method = "GET"
    message = f"{timestamp}{method}{path_to_sign}".encode('utf-8')
    
    # Sign the message with your private key
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    
    # Base64-encode the signature
    encoded_signature = base64.b64encode(signature).decode('utf-8')
    
    headers = {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": encoded_signature
    }
    return headers

def connect_to_rabbitmq():
    """Establishes a connection to RabbitMQ and declares the queue."""
    logging.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}...")
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        # Declare the queue. This is idempotent (won't create if it exists)
        # durable=True means the queue will survive a RabbitMQ restart
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        
        logging.info(f"RabbitMQ connected and '{RABBITMQ_QUEUE}' queue is ready.")
        return connection, channel
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"--- FATAL: Could not connect to RabbitMQ ---: {e}")
        logging.error("Please ensure RabbitMQ is running (use the Docker command).")
        return None, None

async def listen_to_kalshi():
    """
    Main function to connect to Kalshi, authenticate,
    and publish messages to RabbitMQ.
    """
    
    # 1. Load the private key (only need to do this once)
    private_key = load_private_key(PRIVATE_KEY_PATH)
    if not private_key:
        return

    # 2. Connect to RabbitMQ (only need to do this once)
    rabbit_conn, rabbit_channel = connect_to_rabbitmq()
    if not rabbit_conn:
        return

    # This loop will run forever and try to reconnect if disconnected
    while True:
        try:
            # Generate fresh auth headers *inside* the loop
            logging.info("Generating new authentication headers...")
            auth_headers = create_auth_headers(private_key, KALSHI_API_KEY_ID)

            logging.info(f"Connecting to Kalshi WebSocket at {KALSHI_WSS_URL}...")
            
            async with websockets.connect(
                KALSHI_WSS_URL,
                extra_headers=auth_headers
            ) as websocket:
                
                logging.info("WebSocket connected! Subscribing to trade channel...")
                
                subscribe_msg = {
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {"channels": ["trade"]}
                }
                await websocket.send(json.dumps(subscribe_msg))

                # 5. Listen for messages
                async for message in websocket:
                    data = json.loads(message)
                    
                    if data.get('type') == 'trade':
                        
                        # --- !!! THIS IS THE FINAL COMBINED FIX !!! ---
                        
                        trade_msg_data = data.get('msg')
                        trades_list = [] # Start with an empty list

                        if isinstance(trade_msg_data, list):
                            # It's a list of trade objects
                            trades_list = trade_msg_data
                        elif isinstance(trade_msg_data, dict):
                            # It's a single trade object, put it in a list
                            trades_list = [trade_msg_data]
                        else:
                            # It's something unexpected, skip
                            logging.warning(f"Received non-list/non-dict 'msg': {trade_msg_data}")
                            continue 
                        
                        # --- End of normalization ---

                        # Now we can safely iterate over our new 'trades_list'
                        for trade_object in trades_list:
                            if not isinstance(trade_object, dict):
                                logging.warning(f"Skipping malformed trade item: {trade_object}")
                                continue
                            
                            # *** THE FIX: Look for 'market_ticker' ***
                            ticker = trade_object.get('market_ticker')
                            
                            if not ticker:
                                # This warning will now only trigger if 'market_ticker' is truly missing
                                logging.warning(f"Trade object missing 'market_ticker': {trade_object}")
                                continue

                            logging.info(f"TRADE DETECTED: {ticker}")
                            
                            # 6. Publish EACH trade as its own message
                            # We send the whole 'trade_object'
                            rabbit_channel.basic_publish(
                                exchange='',
                                routing_key=RABBITMQ_QUEUE,
                                body=json.dumps(trade_object), # Send the individual trade dict
                                properties=pika.BasicProperties(
                                    delivery_mode=2,  # Make message persistent
                                )
                            )
                        # --- !!! END OF FIX !!! ---
                        
                    elif data.get('type') == 'subscribed':
                        logging.info(f"Successfully subscribed to channel: {data['msg']['channel']}")
                    
                    elif data.get('id') == 1 and data.get('type') == 'error':
                        logging.error(f"Failed to subscribe: {data['msg']}")

        except websockets.exceptions.ConnectionClosed as e:
            logging.warning(f"WebSocket connection closed: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"An error occurred: {e}. Raw message: {message}. Reconnecting in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(listen_to_kalshi())