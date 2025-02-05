from google.cloud import logging as cloud_logging
import logging

def setup_cloud_logger(log_file_name="custom_logger"):
    """
    Sets up Google Cloud Logging with a specific log name.
    Params:
    log_file_name (str): The name of the custom log in Google Cloud.
    """
   
    cloud_client = cloud_logging.Client()

    cloud_logger = cloud_client.logger(log_file_name)

    cloud_client.setup_logging()

    logging.basicConfig(
        format="[%(asctime)s] %(levelname)s - %(message)s", level=logging.INFO
    )

    logging.info(f"Google Cloud Logging is set up with custom log: {log_file_name}")
    return cloud_logger

if __name__ == "__main__":
    logger = setup_cloud_logger("my_custom_log")
    logger.log_text("This log will appear in the 'my_custom_log' log in Google Cloud.")
    logging.warning("This warning will also appear in Google Cloud Logging (root logger).")
