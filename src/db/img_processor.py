import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from ultralytics import YOLO
import logging

# Setup logging
logging.basicConfig(
    filename='/app/logs/yolo_processor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('/app/logs/yolo_processor.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    host='db'
)
cursor = conn.cursor()

# Create raw.image_detections table
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.image_detections (
        detection_id SERIAL PRIMARY KEY,
        message_id BIGINT,
        image_path VARCHAR(255),
        detected_object_class VARCHAR(100),
        confidence_score FLOAT
    );
""")

# Load YOLOv8 model
model = YOLO('yolov8n.pt')  # Pre-trained YOLOv8 nano model

# Scan images in Data Lake
data_lake_path = os.getenv('DATA_LAKE_PATH', '/app/data/raw')
images_path = Path(data_lake_path) / 'telegram_images'

for image_path in images_path.rglob('*.jpg'):
    try:
        # Extract message_id from image filename (e.g., 101_20250710.jpg)
        message_id = int(image_path.stem.split('_')[0])
        
        # Run YOLOv8 detection
        results = model(image_path)
        
        # Process detections
        for result in results:
            for box in result.boxes:
                class_id = int(box.cls)
                class_name = model.names[class_id]
                confidence = float(box.conf)
                
                cursor.execute("""
                    INSERT INTO raw.image_detections (
                        message_id, image_path, detected_object_class, confidence_score
                    ) VALUES (%s, %s, %s, %s);
                """, (
                    message_id,
                    str(image_path),
                    class_name,
                    confidence
                ))
        logger.info(f"Processed image: {image_path}")
    except Exception as e:
        logger.error(f"Error processing {image_path}: {str(e)}")

# Commit and close
conn.commit()
cursor.close()
conn.close()