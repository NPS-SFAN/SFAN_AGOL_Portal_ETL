import logging
"""
log_config.py
log file configuration script

"""
def setup_logging():
    # Configure logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='ETLProcessingLog.log',  # Log to a file
                        filemode='w')        # Append to the log file

# Call the setup function to configure logging
setup_logging()

# Create a module-level logger
logger = logging.getLogger(__name__)