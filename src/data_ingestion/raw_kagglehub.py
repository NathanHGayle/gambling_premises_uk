import kagglehub
from pathlib import Path
from google.cloud.storage import Client, transfer_manager
from src.utils.gc_logger import setup_cloud_logger

def download_kaggle(input_logger,data_url="nathanhg/uk-gam-datasets"):
    input_logger.info("----------- Downloading uk-gam-datasets folder from KaggleHub ---------")
    path = kagglehub.dataset_download(data_url)
    input_logger.info("Path to kaggle dataset files:", path)
    return path


def upload_many_blobs_with_transfer_manager(input_logger,
    bucket_name, filenames, source_directory="", workers=8):
    """Upload every file in a list to a bucket, concurrently in a process pool.

    Each blob name is derived from the filename, not including the
    `source_directory` parameter. For complete control of the blob name for each
    file (and other aspects of individual blob metadata), use
    transfer_manager.upload_many() instead.
    """
    input_logger.info("----------- Uploading uk-gam-datasets folder to Google Cloud Storage -------")
    storage_client = Client()
    # bucket = storage_client.bucket(bucket_name)
    bucket = storage_client.get_bucket(bucket_name, timeout=None) #  timeout = 300 / timeout=(3, 10))

    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=source_directory, max_workers=workers
    )

    for name, result in zip(filenames, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.

        if isinstance(result, Exception):
            input_logger.error("Failed to upload {} due to exception: {}".format(name, result))
        else:
            input_logger.info("Uploaded {} to {}.".format(name, "bucketname_removed"))


def main():
    # Logging
    logger = setup_cloud_logger(log_file_name="data_ingestion.log")

    # Kaggle API download
    directory_path = Path(download_kaggle(logger))  
    
    if not any(directory_path.iterdir()):
         logger.info('Kaggle directory is empty')

    # GCS Upload
    files = [file.name for file in directory_path.iterdir() if file.is_file()]

    print('Input the bucket name:')
    bucketname = input() #USE ENVIRONMENT VARIABLE

    upload_many_blobs_with_transfer_manager(
         input_logger= logger,
         bucket_name=bucketname,
         filenames= files, 
         source_directory=directory_path)

 
if __name__ == "__main__":
        main()


