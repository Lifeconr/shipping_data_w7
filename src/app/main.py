

from fastapi import FastAPI
import logging

# Configure basic logging for visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Telegram Data Pipeline API")

@app.get("/")
async def read_root():
    logger.info("Root endpoint accessed.")
    return {"message": "Welcome to the Telegram Data Pipeline API!"}

@app.get("/status")
async def get_status():
    logger.info("Status endpoint accessed.")
    # You can add logic here to check DB connection, scraper status, etc.
    return {"status": "API is running", "version": "1.0.0"}

# You can add an endpoint to trigger the scraper or get its status
# For now, this is just a placeholder. The actual scraper runs via exec.
@app.get("/trigger_scrape")
async def trigger_scrape():
    logger.info("Scrape trigger endpoint accessed. (Functionality not implemented via API yet)")
    return {"message": "Scraper trigger received, check container logs for details."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)