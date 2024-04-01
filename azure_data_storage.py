from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from constant import AZURE_CONNECTION_STRING


def fetch_files(storage_location):
    container_name = storage_location.removeprefix("abfss://").split("@")[0]
    file_path = "/".join(
        storage_location.removeprefix("abfss://").split("@")[1].split("/")[1:]
    )

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_CONNECTION_STRING
    )

    # Get a client to interact with the specified file system
    file_system_client = blob_service_client.get_container_client(container_name)

    blob_list = file_system_client.list_blobs(name_starts_with=file_path)

    all_files = []
    for blob in blob_list:
        if blob.name == file_path:
            continue

        blob_client = file_system_client.get_blob_client(blob)

        # Fetch only file name
        file_name = (blob_client.blob_name).split("/")[-1]
        all_files.append(file_name)

    return all_files


def download_file_from_s3(storage_location, file_name, download_file_path):
    container_name = storage_location.removeprefix("abfss://").split("@")[0]
    file_path = (
        "/".join(storage_location.removeprefix("abfss://").split("@")[1].split("/")[1:])
        + f"/{file_name}"
    )

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_CONNECTION_STRING
    )

    # Get a client to interact with the specified file system
    container_client = blob_service_client.get_container_client(container_name)

    # Get a client to interact with the specified file
    blob_client = container_client.get_blob_client(file_path)

    print(f"Downloading: {blob_client.blob_name} to {download_file_path}")
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())


if __name__ == "__main__":
    storage_location = "abfss://mskl-metastore-container@msklunitycatalogaccount.dfs.core.windows.net/181e4a02-61f5-426a-944d-c22ae53cf27a/volumes/3c78dec5-59dc-4760-a8e0-fa6e0885dd20"

    total_files = fetch_files(storage_location)
    print(total_files)

    file = total_files[0]
    default_download_path = os.path.expanduser(r"~\Downloads")
    local_path = os.path.join(default_download_path, file)

    download_file_from_s3(storage_location, file, local_path)
