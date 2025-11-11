from typing import IO, AnyStr, Iterable
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import os
from deltalake import write_deltalake
import random, time
from deltalake.exceptions import DeltaError
import logging

credential = DefaultAzureCredential()
default_workspace = '408d8412-c973-46aa-8c0c-52dbe1c38ada'
default_container = 'fb736e26-5db5-424b-8125-6f2ab29a83f0'
data_endpoint = os.getenv("STORAGE_DATA_ENDPOINT", f'onelake.dfs.fabric.microsoft.com/{default_workspace}')    
data_url = f"onelake.dfs.fabric.microsoft.com/{default_container}"
delta_token = credential.get_token("https://storage.azure.com/.default").token


service_client = DataLakeServiceClient(f"https://{data_url}", credential=credential)
file_system_client = service_client.get_file_system_client(file_system=default_workspace)


def upload_to_inbound_storage(path: str, content: str | bytes | Iterable[AnyStr] | IO[AnyStr]) -> None:
    """Upload content to Azure Data Lake storage."""
    file_client = file_system_client.get_file_client(path)
    result = file_client.upload_data(content, overwrite=True)
    
    return result


def write_with_retry(df, table, source, max_attempts=4, base_sleep=2.5):
    attempt = 1
    while True:
        try:
            write_delta(df, table=table, source=source)   # your existing helper
            return
        except DeltaError as e:
            msg = str(e)
            if attempt >= max_attempts:
                raise
            sleep_s = base_sleep * (2 ** (attempt-1)) + random.uniform(0, 1.5)
            logging.warning("Transient Delta open/write error (%s). Retry %d/%d in %.1fs",
                            table, attempt, max_attempts, sleep_s)
            time.sleep(sleep_s)
            attempt += 1


def write_delta(data, table, source="Files"):
    """Write documents to Delta Lake."""
    data_path = f"abfss://{default_container}@{data_endpoint}/{source}/{table}"
    #abfss://exploring_local_compute@onelake.dfs.fabric.microsoft.com/Gold.Lakehouse/Tables/customer'
    #abfss://fb736e26-5db5-424b-8125-6f2ab29a83f0@onelake.dfs.fabric.microsoft.com/408d8412-c973-46aa-8c0c-52dbe1c38ada/Files/orderupdates
    # delta_token = credential.get_token("https://storage.azure.com/.default").token
    storage_options = {"bearer_token": delta_token, "use_fabric_endpoint": "true"}
    
    # table, partition_keys = create_arrow_table(documents)
        
    # Construct predicate for overwriting specific partitions
    # quoted_partkeys = ','.join(f"'{key}'" for key in partition_keys)
    # predicate = f"id in ({quoted_partkeys})"    
    # write_deltalake(
    #     "abfss://fb736e26-5db5-424b-8125-6f2ab29a83f0@onelake.dfs.fabric.microsoft.com/408d8412-c973-46aa-8c0c-52dbe1c38ada/Tables/stream/testfromfunc", 
    #     data, 
    #     mode='overwrite', 
    #     schema_mode='overwrite',
    #     storage_options=storage_options
    # )

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