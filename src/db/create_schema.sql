CREATE TABLE channels (
  channel_id BIGINT PRIMARY KEY,
  title VARCHAR(255),
  username VARCHAR(255)
);
CREATE TABLE messages (
  message_id BIGINT PRIMARY KEY,
  channel_id BIGINT REFERENCES channels(channel_id),
  date DATE,
  text TEXT,
  media_path VARCHAR(255),
  channel_title VARCHAR(255)
);