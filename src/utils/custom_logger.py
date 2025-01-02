import logging
import os


def setup_logger(log_file_name=None, log_dir='logs'):
    """
    Sets up a custom logger with file name.
    Params:
    log_file_name (str): The name of the log file. If not provided, defaults to a timestamped file.
    log_dir (str): Directory where logs will be stored. Defaults to 'logs'.
    """
    logs_path = os.path.join(os.getcwd(),log_dir) # ammend this to fire to gambling_premises_uk\logs
    os.makedirs(logs_path, exist_ok=True)

    log_file_path = os.path.join(logs_path, log_file_name)

    logger = logging.getLogger()  
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

        # Create file handler for logging to the file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info(f"Logger initialized with file: {log_file_path}")

    return logger

if __name__ == "__main__":
    logger = setup_logger(log_file_name="custom_logger.log")
    logger.info("Logging has started for custom_logger.")