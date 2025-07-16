# kafka/producer1.py
from kafka import KafkaProducer
import pandas as pd
import json
import time
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    # Initialize Kafka producer with better retry settings
    logger.info("Connecting to Kafka...")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=10,
        acks='all',
        request_timeout_ms=30000,
        max_block_ms=60000
    )
    
    logger.info("Kafka producer initialized successfully")
    
    # Load the Kaggle credit card fraud dataset
    logger.info("Loading Kaggle fraud dataset...")
    df = pd.read_csv('/home/hamza/DE/20212619_DS463_final/financial-transactions-project/datasets/kaggle_fraud.csv')
    logger.info(f"Loaded {len(df)} records from Kaggle fraud dataset")
    
    # Sample a smaller subset for testing if the dataset is large
    if len(df) > 1000:
        sample_size = min(1000, int(len(df) * 0.1))  # 10% or max 1000 records
        df = df.sample(n=sample_size, random_state=42)
        logger.info(f"Sampled {len(df)} records for streaming")
    
    # Stream data to Kafka
    logger.info("Starting to stream data to Kafka topic 'transactions'...")
    for i, (_, row) in enumerate(df.iterrows()):
        record = row.to_dict()
        
        # Add source identifier
        record['data_source'] = 'kaggle_fraud'
        
        # Send to Kafka
        producer.send('transactions', record)
        
        # Log progress periodically
        if (i + 1) % 100 == 0:
            logger.info(f"Sent {i + 1} records to Kafka")
        
        # Introduce a small delay to avoid overwhelming the system
        time.sleep(0.1)
    
    # Ensure all messages are sent
    producer.flush()
    logger.info("Finished streaming Kaggle fraud dataset to Kafka")
    
except Exception as e:
    logger.error(f"Error in Kafka producer: {str(e)}")
    sys.exit(1)