CREATE TABLE IF NOT EXISTS weather_logs (
  id SERIAL PRIMARY KEY,
  station_id VARCHAR(50),
  temperature FLOAT,
  humidity FLOAT,
  wind_speed FLOAT,
  timestamp TIMESTAMP,
  received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
