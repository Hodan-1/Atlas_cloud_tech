# workers/data_loader/data_loader.py
import os
import sys
import json
import time
import logging
import glob
import uproot
import awkward as ak
import pika

# Add app directory to path
sys.path.append('/app')
from __init__ import connect_to_rabbitmq, serialize_data
from __init__ import PROCESS_QUEUE
from constants import FILE_TO_SAMPLE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_loader')

def load_root_file(file_path):
    """Load data from a ROOT file"""
    logger.info(f"Loading ROOT file: {file_path}")
    
    try:
        # Extract sample type from filename
        filename = os.path.basename(file_path)
        sample_type = FILE_TO_SAMPLE.get(filename)
        
        if not sample_type:
            logger.warning(f"Unknown sample type for file: {filename}")
            return None
        
        # Open ROOT file
        with uproot.open(file_path) as f:
            # Get events tree
            events = f["events"]
            
            # Convert to awkward array
            data = events.arrays()
            
            # Add sample type
            data.sample_type = sample_type
            
            logger.info(f"Loaded {len(data)} events from {file_path}")
            return data
            
    except Exception as e:
        logger.error(f"Error loading ROOT file {file_path}: {e}")
        return None

def main():
    """Main function to load data and send to processor"""
    # Wait for RabbitMQ to be ready
    time.sleep(10)
    
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=PROCESS_QUEUE, durable=True)
    
    # Get environment variables
    lumi = float(os.environ.get('LUMI', 10))
    fraction = float(os.environ.get('FRACTION', 1.0))
    
    # Find ROOT files
    data_dir = '/app/data'
    root_files = glob.glob(f"{data_dir}/*.root")
    
    if not root_files:
        logger.warning("No ROOT files found. Generating test data...")
        # Import and run the test data generator
        sys.path.append('/app')
        from generate_test_data import main as generate_data
        generate_data()
        
        # Find the newly generated files
        root_files = glob.glob(f"{data_dir}/*.root")
    
    logger.info(f"Found {len(root_files)} ROOT files")
    
    # Process each file
    for file_path in root_files:
        # Load data
        data = load_root_file(file_path)
        
        if data is None:
            continue
        
        # Serialize data
        serialized_data = serialize_data(data)
        
        # Send to processor
        channel.basic_publish(
            exchange='',
            routing_key=PROCESS_QUEUE,
            body=json.dumps({
                'file': os.path.basename(file_path),
                'sample_type': data.sample_type,
                'data': serialized_data,
                'lumi': lumi,
                'fraction': fraction,
                'timestamp': time.time()
            }),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        
        logger.info(f"Sent {os.path.basename(file_path)} to processor")
    
    # Close connection
    connection.close()
    logger.info("Data loading complete")

if __name__ == "__main__":
    main()
