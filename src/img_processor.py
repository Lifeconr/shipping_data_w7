# image_processor.py
import os
import json
from datetime import datetime
from ultralytics import YOLO
from PIL import Image
import psycopg2
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load YOLO model
model = YOLO('yolov8n.pt')  # Using nano version for efficiency

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(os.getenv('DATABASE_URL'))

def process_image(image_path):
    """Process an image with YOLO and return detections"""
    try:
        results = model(image_path)
        detections = []
        
        for result in results:
            for box in result.boxes:
                detections.append({
                    'class_id': int(box.cls),
                    'class_name': model.names[int(box.cls)],
                    'confidence': float(box.conf),
                    'bbox': box.xywhn.tolist()[0]  # Normalized xywh coordinates
                })
        
        return detections
    
    except Exception as e:
        logger.error(f"Error processing image {image_path}: {str(e)}")
        return []

def save_detections_to_db(message_id, detections):
    """Save detection results to database"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        for detection in detections:
            cur.execute("""
                INSERT INTO image_detections 
                (message_id, class_id, class_name, confidence, bbox)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    message_id,
                    detection['class_id'],
                    detection['class_name'],
                    detection['confidence'],
                    json.dumps(detection['bbox'])
                )
            )
        
        conn.commit()
        logger.info(f"Saved {len(detections)} detections for message {message_id}")
    
    except Exception as e:
        conn.rollback()
        logger.error(f"Error saving detections to DB: {str(e)}")
    
    finally:
        cur.close()
        conn.close()

def get_messages_with_images():
    """Retrieve messages with images that haven't been processed yet"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT m.message_id, m.media_path
            FROM stg_telegram_messages m
            LEFT JOIN image_detections d ON m.message_id = d.message_id
            WHERE m.has_media = TRUE 
            AND m.media_type = 'photo'
            AND d.message_id IS NULL
            AND m.media_path IS NOT NULL
            """)
        
        return cur.fetchall()
    
    except Exception as e:
        logger.error(f"Error fetching messages with images: {str(e)}")
        return []
    
    finally:
        cur.close()
        conn.close()

def main():
    messages = get_messages_with_images()
    logger.info(f"Found {len(messages)} messages with images to process")
    
    for message_id, image_path in messages:
        if os.path.exists(image_path):
            detections = process_image(image_path)
            if detections:
                save_detections_to_db(message_id, detections)
        else:
            logger.warning(f"Image path does not exist: {image_path}")

if __name__ == '__main__':
    main()