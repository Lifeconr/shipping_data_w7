import os
import asyncio
import json
import logging
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv
from pathlib import Path

# Setup logging
logging.basicConfig(
    filename='logs/telegram_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/telegram_scraper.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE')
session_name = os.getenv('TELEGRAM_SESSION_NAME', 'telegram_session')
data_lake_path = os.getenv('DATA_LAKE_PATH', 'data/raw')

# Data Lake paths
messages_path = os.path.join(data_lake_path, 'telegram_messages')

# Channels to scrape
CHANNELS = ['chemed_chem', 'lobelia4cosmetics', 'tikvahpharma']

# Sample data for testing
SAMPLE_MESSAGES = [
    {
        'channel': 'chemed_chem',
        'id': 101,
        'date': '2025-07-10',
        'message': 'Paracetamol 500mg available at 50 ETB',
        'media': 'photo_paracetamol.jpg'
    },
    {
        'channel': 'lobelia4cosmetics',
        'id': 102,
        'date': '2025-07-11',
        'message': 'Antibiotic cream for sale',
        'media': None
    },
    {
        'channel': 'tikvahpharma',
        'id': 103,
        'date': '2025-07-12',
        'message': 'Insulin stock limited, order now',
        'media': 'photo_insulin.jpg'
    }
]

def setup_data_lake():
    """Create Data Lake directory structure."""
    Path(messages_path).mkdir(parents=True, exist_ok=True)
    logger.info(f"Data Lake initialized at {messages_path}")

async def scrape_channel(client, channel):
    """Scrape messages from a Telegram channel."""
    try:
        entity = await client.get_entity(f'https://t.me/{channel}')
        messages = []
        async for message in client.iter_messages(entity, limit=50):
            msg_data = {
                'channel': channel,
                'id': message.id,
                'date': message.date.strftime('%Y-%m-%d'),
                'message': message.text or '',
                'media': 'photo' if isinstance(message.media, MessageMediaPhoto) else None
            }
            messages.append(msg_data)
            await asyncio.sleep(0.5)  # Avoid rate limits
        logger.info(f"Scraped {len(messages)} messages from {channel}")
        return messages
    except Exception as e:
        logger.error(f"Error scraping {channel}: {str(e)}")
        return []

def save_to_data_lake(messages):
    """Save messages to Data Lake as JSON."""
    for msg in messages:
        date = msg['date']
        channel = msg['channel']
        dir_path = Path(messages_path) / date / channel
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{msg['id']}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(msg, f, indent=4)
        logger.info(f"Saved message {msg['id']} to {file_path}")

async def main():
    """Main function to scrape Telegram channels."""
    setup_data_lake()
    
    if not (api_id and api_hash and phone):
        logger.warning("Using sample data due to missing Telegram API credentials")
        save_to_data_lake(SAMPLE_MESSAGES)
        return
    
    async with TelegramClient(session_name, api_id, api_hash) as client:
        await client.start(phone=phone)
        for channel in CHANNELS:
            messages = await scrape_channel(client, channel)
            save_to_data_lake(messages)
            await asyncio.sleep(10)  # Delay between channels

if __name__ == "__main__":
    asyncio.run(main())