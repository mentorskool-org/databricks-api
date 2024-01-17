import os
from s3_methods import get_s3_data
import delta_sharing

import json
import pandas as pd

BUCKET = "aws-databricks-credentials"
CREDENTIALS_FILE_KEY = "config/config.share"


def create_temporary_file(credentials_content):
    # Check if running in AWS Lambda
    if "LAMBDA_RUNTIME_DIR" in os.environ:
        # If in Lambda, use /tmp directory
        temp_file_path = "/tmp/delta_sharing_credentials.json"

        # Create a temporary file and write the credentials content to it
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(credentials_content)
    else:
        # If running locally, use the current working directory
        temp_file_path = "./tmp/delta_sharing_credentials.json"

        # Create a temporary file and write the credentials content to it
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(credentials_content)

    print(f"Temporary file path: {temp_file_path}")
    return temp_file_path


def get_table_url(share_name, schema_name, table_name, credentials_content):
    # Get the file data from s3, store it in a temporary file
    profile_file = create_temporary_file(credentials_content)

    table_url = profile_file + f"#{share_name}.{schema_name}.{table_name}"

    return table_url


def get_catalog_data(share_name, schema_name, table_name):
    credentials_content = get_s3_data(bucket_name=BUCKET, key=CREDENTIALS_FILE_KEY)

    table_url = get_table_url(share_name, schema_name, table_name, credentials_content)

    # We will load the whole table
    data_df = delta_sharing.load_as_pandas(table_url)

    # fetch the table_metadata
    meta_data = delta_sharing.get_table_metadata(table_url)
    table_schema = json.loads(
        meta_data.__getattribute__("schema_string")
    )

    # Save it in a csv file
    meta_data_df = pd.json_normalize(table_schema["fields"])

    # once done remove that file
    delete_tmp_file()

    # Return both data and metadata
    data_and_metadata_info = {
        "data": data_df,
        "metadata": meta_data_df
    }

    return data_and_metadata_info


def delete_tmp_file():
    if "LAMBDA_RUNTIME_DIR" in os.environ:
        # If in Lambda, use /tmp directory
        temp_file_path = "/tmp/delta_sharing_credentials.json"
    else:
        # If running locally, use the current working directory
        temp_file_path = "./tmp/delta_sharing_credentials.json"

    os.remove(temp_file_path)


if __name__ == "__main__":
    share_name = "all_share"
    schema_name = "globalmart_de"
    table_name = "customers"
    data_and_metadata_info = get_catalog_data(share_name, schema_name, table_name)
    table_data_df = pd.DataFrame(data_and_metadata_info["data"])
    table_metadata_df = pd.DataFrame(data_and_metadata_info["metadata"])
    
    print(table_data_df.head(5))
    print(table_metadata_df.head(5))
