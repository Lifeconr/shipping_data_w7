import os
import json
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

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

# Create raw schema and table
cursor.execute("""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        message_id BIGINT,
        channel_id BIGINT,
        date DATE,
        text TEXT,
        media_path VARCHAR(255),
        media_type VARCHAR(50),
        channel_title VARCHAR(255),
        channel_username VARCHAR(255),
        raw_json JSONB
    );
""")

# Load JSON files from Data Lake
data_lake_path = os.getenv('DATA_LAKE_PATH', '/app/data/raw')
messages_path = Path(data_lake_path) / 'telegram_messages'

for file_path in messages_path.rglob('*.json'):
    with open(file_path, 'r', encoding='utf-8') as f:
        msg = json.load(f)
        cursor.execute("""
            INSERT INTO raw.telegram_messages (
                message_id, channel_id, date, text, media_path, media_type,
                channel_title, channel_username, raw_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (
            msg.get('id'),
            msg.get('channel_id'),
            msg.get('date'),
            msg.get('message'),
            msg.get('media_path'),
            msg.get('media_type'),
            msg.get('channel_title'),
            f"@{msg.get('channel')}",
            json.dumps(msg)
        ))

# Commit and close
conn.commit()
cursor.close()
conn.close()