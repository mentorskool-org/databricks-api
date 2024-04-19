from azure.storage.blob import BlobServiceClient
from constant import AZURE_CONNECTION_STRING, CLUSTER_ID
import fetch_catalog_data as fc
import csv

import time
from datetime import datetime


def upload_file_to_volume(storage_location: str, local_file_path: str):
    print(local_file_path)

    container_name = storage_location.removeprefix("abfss://").split("@")[0]
    directory_path = "/".join(
        storage_location.removeprefix("abfss://").split("@")[1].split("/")[1:]
    )

    file_name = local_file_path.split("\\")[-1]
    file_path = f"{directory_path}/{file_name}"

    # local_file_path = r"C:\Users\burha\Downloads\SQL_Scenarios_200.csv"

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_CONNECTION_STRING
    )

    # Get a client to interact with the specified file system
    container_client = blob_service_client.get_container_client(container_name)

    # Create a blob client
    blob_client = container_client.get_blob_client(file_path)

    # Upload the local file to Azure Blob Storage
    with open(local_file_path, "rb") as file:
        blob_client.upload_blob(file, overwrite=True)

    print(f"File '{local_file_path}' uploaded to Azure Blob Storage.")

    return f"File **{file_name}** is uploaded to Azure Blob Storage."


def is_csv(file_path):
    # Accept only .csv file
    file_name = file_path.split("\\")[-1]
    if file_name.endswith(".csv"):
        return True

    return False


def upload_file_to_table(
    catalog_name, database_name, table_name, file_path, description
):
    print(file_path)

    file_type = is_csv(file_path)

    if not file_type:
        return False

    print(f"{datetime.now()} File reading started!")

    # Let's fetch the csv data and check that when we pass csv and convert it into dataframe and then load, so it get's loaded or not
    try:
        data = []
        with open(file_path, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                data.append(row)
    except FileNotFoundError as error:
        raise FileNotFoundError(
            f"There is no such file: {file_path}. Please Enter the correct file path!"
        )

    print(f"{datetime.now()} File reading completed!")

    # Check the status of the cluster
    while True:
        cluster_state = fc.get_cluster_state(CLUSTER_ID)
        print(f"The cluster state is: {cluster_state}")
        if cluster_state == "RUNNING":
            break

        # TODO: For now, we are manually creating the cluster, but discuss that shall we create cluster via code, if it not doesn't  exists
        # Start the cluster
        fc.start_cluster(CLUSTER_ID)
        time.sleep(
            60
        )  # wait for 60 seconds and check whether cluster is started or not

    # Create the context
    context_id = fc.create_execution_context(CLUSTER_ID, "python")
    print(f"{datetime.now()} Context is created!")

    # Create the context
    # context_id = fc.create_execution_context(CLUSTER_ID, "python")

    command = f"""
        import pandas as pd

        pandas_df = pd.DataFrame({data})
        
        spark_df = spark.createDataFrame(pandas_df)

        spark_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "True") \
            .option("userMetadata", "{description}") \
            .saveAsTable("{catalog_name}.{database_name}.{table_name}")
    """

    # Once the context is created, execute the command
    command_id = fc.execute_command(CLUSTER_ID, context_id, command, "python")
    print(f"{datetime.now()} Command executed!")

    # Once command is executed, fetch the total_rows
    while True:
        response_json = fc.get_command_execution_output(
            fc.CLUSTER_ID, context_id, command_id
        )
        if response_json.get("status"):
            if response_json["status"] == "Finished":
                if response_json.get("results"):
                    if response_json["results"]["resultType"] == "error":
                        raise Exception(f"Error: {response_json['results']['summary']}")
                
                print(f"{datetime.now()} Data uploaded!")
                return response_json


def update_comments(
    catalog_name: str, database_name: str, table_name: str, columns_comments_dict: dict
):
    while True:
        cluster_state = fc.get_cluster_state(CLUSTER_ID)
        print(f"The cluster state is: {cluster_state}")
        if cluster_state == "RUNNING":
            break

        # TODO: For now, we are manually creating the cluster, but discuss that shall we create cluster via code, if it not doesn't  exists
        # Start the cluster
        fc.start_cluster(CLUSTER_ID)
        time.sleep(
            60
        )  # wait for 60 seconds and check whether cluster is started or not

    # Create context
    context_id = fc.create_execution_context(fc.CLUSTER_ID, "python")

    for col in columns_comments_dict:
        command = f"""
            -- Let's change the property again and check it's version
            ALTER TABLE {catalog_name}.{database_name}.{table_name} 
                ALTER {col} COMMENT '{columns_comments_dict[col]}'
        """

        # execute command
        command_id = fc.execute_command(fc.CLUSTER_ID, context_id, command, "sql")

        # fetch result and wait until the status == finished, then move ahead
        while True:
            response_json = fc.get_command_execution_output(
                fc.CLUSTER_ID, context_id, command_id
            )
            if response_json.get("status"):
                if response_json["status"] == "Finished":
                    print(response_json)
                    break


if __name__ == "__main__":
    # storage_location = "abfss://mskl-metastore-container@msklunitycatalogaccount.dfs.core.windows.net/181e4a02-61f5-426a-944d-c22ae53cf27a/volumes/3c78dec5-59dc-4760-a8e0-fa6e0885dd20"
    # local_file_path = ""
    # upload_file_to_volume()

    # catalog_name = "auto_insurance"
    # database_name = "default"
    # table_name = "customers"

    # # let's test the update_comment function
    # columns = {
    #     "CustomerID": "This is a customer_id column uniquely describing the customers - 3",
    #     "State": "this is state - 2",
    #     "City": "City - 2",
    # }
    # response = update_comments(catalog_name, database_name, table_name, columns)

    catalog_name = 'cpg'
    schema_name = 'data_engineering'
    table_name = 'customers'
    file_to_upload_location = r"C:\Users\burha\Mentorskool\Enqurious\Available Data\Cpg\customers.csv"
    description = 'completely new data for testing purpose'
    upload_file_to_table(
        catalog_name, schema_name, table_name, file_to_upload_location, description
    )
