import os
from dotenv import load_dotenv
import pika
import json
import logging
import discord
import asyncio
import threading
from datetime import datetime

load_dotenv()

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = 'localhost'
ALERTS_QUEUE = 'alerts'  # The queue we read from

BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN')

if not BOT_TOKEN:
    logging.error("FATAL: No Bot Token found! Check your .env file.")
    exit(1)

CHANNEL_MAP = {
    "Sports": 1436206256304947291,
    "Politics": 1436206275074457680,
    "Social": 1436207114191376434,
    "Crypto": 1436207303828439050,
    "Climate and Weather": 1436207381737635890,
    "Economics": 1436207399076757524,
    "Mentions": 1436207648650297435,
    "Companies": 1436207665788354581,
    "Financials": 1436207685753241620,
    "Health": 1436444752865398794,
    "Transportation": 1436444940107382994,
    "World": 1436445110802976940,
    "Science and Technology": 1436445424440315924,
    "Elections": 1436445741584351232,
    "Education": 1436445954449342574,
    "Entertainment": 1436446380330713240, 
    "Other": 1436214343074320454 
}

# --- Discord Bot Setup ---
intents = discord.Intents.default()
client = discord.Client(intents=intents)

# This queue will act as a bridge between RabbitMQ (sync) and Discord (async)
async_alert_queue = asyncio.Queue()


def rabbitmq_consumer_thread():
    """
    This function runs in a separate thread.
    It blocks and waits for messages from RabbitMQ.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=ALERTS_QUEUE, durable=True)
        logging.info("RabbitMQ thread: Connected and waiting for alerts...")

        def callback(ch, method, properties, body):
            """This is called by pika when a message is received."""
            try:
                logging.info("RabbitMQ thread: Alert received!")
                alert_msg = json.loads(body)
                
                # Get the bot's running event loop
                loop = client.loop
                
                # Put the message into the async queue for Discord to handle
                # We must use 'call_soon_threadsafe' because this is
                # a different thread from the main asyncio loop.
                asyncio.run_coroutine_threadsafe(
                    async_alert_queue.put(alert_msg),
                    loop
                )
                
            except Exception as e:
                logging.error(f"Error processing RabbitMQ message: {e}")
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=ALERTS_QUEUE, on_message_callback=callback)
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"--- FATAL (RabbitMQ Thread): Could not connect: {e} ---")
    except Exception as e:
        logging.error(f"--- FATAL (RabbitMQ Thread): {e} ---")


#
# --- REPLACE YOUR OLD discord_alert_sender FUNCTION WITH THIS ---
#
async def discord_alert_sender():
    """
    This is an async task that waits for items in the
    async_alert_queue and sends them to the correct Discord channel.

    This function is started by the 'on_ready' event.
    """
    await client.wait_until_ready() # Wait for the bot to be logged in

    logging.info("Discord alert sender is ready and listening for alerts...")

    # Main loop: wait for an alert and send it
    while True:
        try:
            # Wait for a message from the RabbitMQ thread
            alert_msg = await async_alert_queue.get()

            # --- !!! NEW ROUTING LOGIC !!! ---

            # 1. Get the category from the alert message.
            # .title() makes "politics" -> "Politics" to match our map keys
            category = alert_msg.get('category', 'Other').title()

            logging.info(f"DEBUG X-RAY: Alerter read category as: '{category}'. Trying to route to ID: {CHANNEL_MAP.get(category)}")

            # 2. Find the correct Channel ID.
            # If the category isn't in our map, use the "Other" channel's ID.
            target_channel_id = CHANNEL_MAP.get(category, CHANNEL_MAP['Other'])

            # 3. Get the channel object from Discord.
            channel = client.get_channel(target_channel_id)

            if not channel:
                logging.error(f"Could not find channel for category '{category}' (ID: {target_channel_id}). Sending to 'Other'...")
                # Fallback to the 'Other' channel if the ID was wrong
                channel = client.get_channel(CHANNEL_MAP['Other'])
                if not channel:
                    logging.error(f"--- FATAL: Could not even find 'Other' channel. Check your CHANNEL_MAP. ---")
                    continue # Skip this alert

            # --- !!! END OF ROUTING LOGIC !!! ---

            logging.info(f"Sending alert for '{category}' to channel: #{channel.name}")

            # --- Format the Discord Message ---
            score = alert_msg.get('anomaly_score', -1)

            if score < -0.7:
                color = discord.Color.red()
                title = "🚨🚨🚨 EXTREME WHALE DETECTED 🚨🚨🚨"
            elif score < -0.65:
                color = discord.Color.orange()
                title = "🔥 Major Whale Detected! 🔥"
            else:
                color = discord.Color.gold()
                title = "Whale Detected!"

            embed = discord.Embed(
                title=title,
                description=f"**{alert_msg.get('title')}**",
                url=alert_msg.get('url'),
                color=color,
                timestamp=datetime.fromisoformat(alert_msg.get('trade_time'))
            )

            embed.add_field(
                name="Market Ticker", 
                value=f"`{alert_msg.get('ticker')}`", 
                inline=False
            )
            embed.add_field(
                name="Anomaly Score", 
                value=f"**{score:.4f}**", 
                inline=True
            )
            embed.add_field(
                name="Bet Placed", 
                value=f"**{alert_msg.get('trade_size'):,}** contracts of **`{alert_msg.get('taker_side', 'N/A').upper()}`** at **{alert_msg.get('price_paid')}¢** each",
                inline=False
            )

            await channel.send(embed=embed)

        except Exception as e:
            logging.error(f"Error in Discord sender task: {e}")
#
# --- END OF REPLACEMENT ---
#

# --- THIS IS THE NEW STARTUP LOGIC ---

@client.event
async def on_ready():
    """
    This is called by discord.py when the bot has successfully logged in.
    """
    logging.info(f"Discord bot is ready. Logged in as: {client.user}")
    # Start the background task that sends alerts
    asyncio.create_task(discord_alert_sender())

def main():
    """Main function to start the RabbitMQ thread and the Discord bot."""
    
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logging.error("--- ERROR: Please fill in your BOT_TOKEN and CHANNEL_ID in alerter.py ---")
        return
        
    logging.info("Starting RabbitMQ consumer thread...")
    # Start the RabbitMQ consumer in a separate daemon thread
    # This thread will run in the background
    rabbit_thread = threading.Thread(target=rabbitmq_consumer_thread, daemon=True)
    rabbit_thread.start()
    
    logging.info("Starting Discord bot...")
    # This call is BLOCKING. It will run forever until Ctrl+C
    # It runs the bot's internal async event loop
    try:
        client.run(BOT_TOKEN)
    except discord.errors.LoginFailure:
        logging.error("--- FATAL: Discord login failed. Check your BOT_TOKEN. ---")
    except KeyboardInterrupt:
        logging.info("Shutting down Alerter...")

if __name__ == "__main__":
    main() # Call the new synchronous main function