#logging_config.py
import logging
import logging.config
import yaml
import os

def setup_logging(config_path: str = 'config/logging_config.yaml'):
    """Set up logging using a YAML configuration file."""
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logging.config.dictConfig(config)
            logger = logging.getLogger(__name__)
            logger.info(f"Logging configuration loaded from {config_path}.")
    else:
        raise FileNotFoundError(f"Logging configuration file not found at {config_path}.")