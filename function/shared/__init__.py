from typing import IO, AnyStr, Iterable
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os
from deltalake import write_deltalake
import random, time
from deltalake.exceptions import DeltaError
import logging

logging.info("Initializing shared module for Azure Data Lake operations.")

credential = DefaultAzureCredential()
default_workspace = '408d8412-c973-46aa-8c0c-52dbe1c38ada'
default_container = 'fb736e26-5db5-424b-8125-6f2ab29a83f0'
data_endpoint = os.getenv("STORAGE_DATA_ENDPOINT", f'onelake.dfs.fabric.microsoft.com/{default_workspace}')    
data_url = f"onelake.dfs.fabric.microsoft.com/{default_container}"


service_client = DataLakeServiceClient(f"https://{data_url}", credential=credential)
file_system_client = service_client.get_file_system_client(file_system=default_workspace)

def get_delta_token() -> str:
    return credential.get_token("https://storage.azure.com/.default").token

def upload_to_inbound_storage(path: str, content: str | bytes | Iterable[AnyStr] | IO[AnyStr]) -> None:
    """Upload content to Azure Data Lake storage."""
    file_client = file_system_client.get_file_client(path)
    result = file_client.upload_data(content, overwrite=True)
    
    return result


def write_with_retry(df, table, source, max_attempts=6, base_sleep=1.5):
    attempt = 1
    while True:
        try:
            write_delta(df, table=table, source=source)   # your existing helper
            return
        except Exception as e:
            msg = str(e)
            if attempt >= max_attempts:
                raise
            sleep_s = base_sleep * (2 ** (attempt-1)) + random.uniform(0, 2.5)
            logging.warning("Transient Delta open/write error (%s). Retry %d/%d in %.1fs",
                            table, attempt, max_attempts, sleep_s)
            time.sleep(sleep_s)
            attempt += 1


def write_delta(data, table, source="Files"):
    """Write documents to Delta Lake."""
    data_path = f"abfss://{default_container}@{data_endpoint}/{source}/{table}"
    logging.info(f"Writing to Delta Lake table: {table} in source: {source}, path: {data_path}")

    logging.info("Getting a new Delta token for write operation.")
    delta_token = get_delta_token()
    storage_options = {"bearer_token": delta_token, "use_fabric_endpoint": "true"}
    
    write_deltalake(
        data_path, 
        data = data, 
        mode='append', 
        schema_mode='merge',
        # partition_by=['id'], 
        # predicate=predicate,
        # engine="rust",
        storage_options=storage_options
    )