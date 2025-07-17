import os
import json
from fastapi import FastAPI
import logging
import asyncio
from pathlib import Path
from src.scraping import telegram_scraper  # Import scraper main function

logging.basicConfig(
    filename='logs/api.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/api.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Telegram Data Pipeline API")

@app.get("/")
async def read_root():
    logger.info("Root endpoint accessed")
    return {"message": "Welcome to the Telegram Data Pipeline API"}

@app.get("/status")
async def get_status():
    logger.info("Status endpoint accessed")
    return {"status": "API running", "version": "1.0.0"}

@app.post("/trigger_scrape")
async def trigger_scrape():
    logger.info("Triggering Telegram scrape")
    try:
        await telegram_scraper.main()
        return {"message": "Scraping completed successfully"}
    except Exception as e:
        logger.error(f"Scraping error: {str(e)}")
        return {"message": f"Scraping failed: {str(e)}"}

@app.get("/messages/{date}/{channel}")
async def get_messages(date: str, channel: str):
    logger.info(f"Fetching messages for {date}/{channel}")
    dir_path = Path(f"/app/data/raw/telegram_messages/{date}/{channel}")
    messages = []
    if dir_path.exists():
        for file_path in dir_path.glob("*.json"):
            with open(file_path, 'r', encoding='utf-8') as f:
                messages.append(json.load(f))
        return {"channel": channel, "date": date, "messages": messages}
    logger.warning(f"No messages found for {date}/{channel}")
    return {"message": "No data found"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)