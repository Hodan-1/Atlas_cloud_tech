# workers/worker.py
# Add to each worker's main function
import os
import time
import threading
import logging
from common import connect_to_rabbitmq, serialize_data, deserialize_data
from common import SAMPLE_QUEUE, PROCESS_QUEUE, ANALYSIS_QUEUE, PLOT_QUEUE, RESULT_QUEUE

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    worker_type = os.environ.get('WORKER_TYPE')
    if not worker_type:
        logger.error("WORKER_TYPE environment variable is not set. Exiting.")
        return
    
    logger.info(f"{worker_type} worker starting...")
    
    # Print heartbeat every 30 seconds
    def heartbeat():
        while True:
            logger.info(f"{worker_type} worker is alive at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(30)
    
    # Start the heartbeat thread
    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Dynamically import and run the worker's main function
    try:
        if worker_type == 'data_loader':
            from data_loader import main as worker_main
        elif worker_type == 'data_processor':
            from data_processor import main as worker_main
        elif worker_type == 'analyzer':
            from analyzer import main as worker_main
        elif worker_type == 'visualizer':
            from visualization import main as worker_main
        else:
            raise ValueError(f"Unknown worker type: {worker_type}")
        
        # Run the worker's main function
        worker_main()
    except ImportError as e:
        logger.error(f"Failed to import worker module for {worker_type}: {e}")
    except Exception as e:
        logger.error(f"Error in {worker_type} worker: {e}")
    finally:
        logger.info(f"{worker_type} worker shutting down...")

if __name__ == "__main__":
    main()