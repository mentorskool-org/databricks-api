import pandas as pd
import requests
import json
import time

from constant import TOKEN, DATABRICKS_HOST


HEADERS = {"Authorization": f"Bearer {TOKEN}"}
CLUSTER_ID = "0201-093503-pg3rgvwi"


# Fetch all catalogs
def fetch_catalogs():
    response = requests.get(f"{DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs", headers=HEADERS)
    
    # Fetch the data
    data = response.json()

    catalogs = []
    for catalog in data['catalogs']: # [0]["name"]
        catalogs.append(catalog["name"])
    
    return catalogs


# Fetch metadata of the table
def get_table_metadata(
    catalog_name: str, database_name: str, table_name: str
) -> pd.DataFrame:
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{catalog_name}.{database_name}.{table_name}?include_delta_metadata=True",
        headers=HEADERS,
    )

    # Fetch the metadata
    data = response.json()

    metadata = []
    for item in data["columns"]:
        metadata.append(json.loads(item["type_json"]))

    metadata_df = pd.json_normalize(metadata)

    return metadata_df


def fetch_schemas(catalog_name: str) -> list:
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/schemas",
        headers=HEADERS,
        params={"catalog_name": catalog_name},
    )

    schemas = [schema_info["name"] for schema_info in response.json()["schemas"]]

    return schemas


def fetch_tables(schema_name, catalog_name: str) -> list:
    # get the command info
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables",
        headers=HEADERS,
        params={"catalog_name": catalog_name, "schema_name": schema_name},
    )

    if response.json():
        # fetch the tables
        tables = [table_info["name"] for table_info in response.json()["tables"]]
    else:
        tables = []

    return tables


def fetch_volume_storage(catalog_name, schema_name, volume_name):
    # get the command info
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.1/unity-catalog/volumes/{catalog_name}.{schema_name}.{volume_name}",
        headers=HEADERS
    )

    # 
    storage_location = response.json()["storage_location"]

    return storage_location



def table_version(schema_name: str, table_name: str, catalog_name: str = "content_datasets") -> list:
    # # get the command info
    # response = requests.get(
    #     f"{DATABRICKS_HOST}/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}",
    #     headers=HEADERS,
    # )

    # generation_value = response.json()["generation"]+1

    # generations = list(range(generation_value))

    # return generations
    while True:
        cluster_state = get_cluster_state(CLUSTER_ID)
        print(f"The cluster state is: {cluster_state}")
        if cluster_state == "RUNNING":
            break

        # TODO: For now, we are manually creating the cluster, but discuss that shall we create cluster via code, if it not doesn't  exists
        # Start the cluster
        start_cluster(CLUSTER_ID)
        time.sleep(
            60
        )  # wait for 60 seconds and check whether cluster is started or not

    # Create the context
    context_id = create_execution_context(CLUSTER_ID, "sql")

    # Execute the following command
    command = f"""
    DESC HISTORY {catalog_name}.{schema_name}.{table_name};
"""
    command_id = execute_command(CLUSTER_ID, context_id, command, "sql")

    while True:
        # Once the command is executed, fetch the result
        response = get_command_execution_output(CLUSTER_ID, context_id, command_id)
        print(response)

        if response.get("results") is None:
            continue

        # Check if data is available in the response
        if response['results']["resultType"] == "error":
            raise Exception(f"Error: {results['error']}")
        
        
        results = response['results']
        if results.get("data") is not None:
            versions = []
            for version in results['data']:
                versions.append(version[0])
                
            return versions
            
    
def get_cluster_state(cluster_id: str) -> str | int:
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/clusters/get",
        headers=HEADERS,
        params={"cluster_id": cluster_id},
    )

    # Return the state of the cluster
    return response.json()["state"]


def start_cluster(cluster_id: str):
    # Create a cluster. It will return empty dataframe
    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/clusters/start",
        headers=HEADERS,
        params={"cluster_id": cluster_id},
    )


def create_execution_context(cluster_id: str, language: str) -> str:
    data = {"clusterId": cluster_id, "language": language}

    context_response = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/contexts/create",
        headers=HEADERS,
        data=json.dumps(data),
    )

    return context_response.json()["id"]


def execute_command(cluster_id: str, context_id: str, command: str, language: str) -> str:
    data = {
        "clusterId": cluster_id,
        "contextId": context_id,
        "language": language,
        "command": command,
    }

    command_response = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/commands/execute",
        headers=HEADERS,
        data=json.dumps(data),
    )

    return command_response.json()["id"]


def get_command_execution_output(
    cluster_id: str, context_id: str, command_id: str
) -> str | list:
    # get the command info
    response = requests.get(
        f"{DATABRICKS_HOST}/api/1.2/commands/status",
        headers=HEADERS,
        params={
            "clusterId": cluster_id,
            "contextId": context_id,
            "commandId": command_id,
        },
    )

    response_json = response.json()

    if response_json.get("result"):
        if response_json["result"]["resultType"] == "error":
            raise Exception(f"Error: {response_json['results']['summary']}")

    return response.json()


def get_total_rows(
    catalog_name: str, database_name: str, table_name: str
):
    # Create the context
    context_id = create_execution_context(CLUSTER_ID, "python")
    
    command = f"""
    spark.sql("SELECT COUNT(*) as total_rows FROM {catalog_name}.{database_name}.{table_name}").collect()[0]['total_rows']
"""

    # Once the context is created, execute the command
    command_id = execute_command(CLUSTER_ID, context_id, command, "python")

    # Wait for 10 seconds and then check the output
    # time.sleep(20)

    while True:
        # Once command is executed, fetch the total_rows
        response = get_command_execution_output(CLUSTER_ID, context_id, command_id)

        # Check if data is available in the response
        if response.get("results") is not None:
            return int(response["results"]["data"])


def fetch_table_data(
    catalog_name: str,
    database_name: str,
    table_name: str,
    total_rows: int,
    version: int
):
    # Create the context
    context_id = create_execution_context(CLUSTER_ID, "python")

    final_data = []
    start_row = 0
    while start_row < total_rows:
        print(f"Start row: {start_row}")

        # Let's calculate the end row
        end_row = start_row + 999
        print(f"End row: {end_row}")

        # Set the limit
        limit = end_row + 1

        # execute the given command
        command = f"""
    # Read all rows from Delta Lake table and add to a Spark DataFrame
    all_data = spark.read.format("delta").table("{catalog_name}.{database_name}.{table_name}@v{version}")

    # Assuming df is your DataFrame
    start_row = {start_row}
    end_row = {end_row}

    # Display rows from start_row to end_row
    display(all_data.limit({limit}).toPandas().tail(end_row - start_row + 1))
"""
        command_id = execute_command(CLUSTER_ID, context_id, command, "python")

        # Wait for 10 seconds and then check the output
        # time.sleep(10)

        # Now fetch the output
        while True:
            # Once command is executed, fetch the total_rows
            response = get_command_execution_output(CLUSTER_ID, context_id, command_id)

            # Check if data is available in the response
            if response.get("results") is not None:
                # Add that into final_data
                final_data.extend(response["results"]["data"])
                break

        # response = get_command_execution_output(CLUSTER_ID, context_id, command_id)

        start_row = end_row + 1

    # Once we get the data, fetch the schema and based on that create the dataframe
    columns = [col["name"] for col in response["results"]["schema"]]
    data_df = pd.DataFrame(final_data, columns=columns)
    return data_df


def main(
    catalog_name: str, database_name: str, table_name: str, version: int
) -> dict[str : pd.DataFrame]:
    # Check the status of the cluster
    while True:
        cluster_state = get_cluster_state(CLUSTER_ID)
        print(f"The cluster state is: {cluster_state}")
        if cluster_state == "RUNNING":
            break

        # TODO: For now, we are manually creating the cluster, but discuss that shall we create cluster via code, if it not doesn't  exists
        # Start the cluster
        start_cluster(CLUSTER_ID)
        time.sleep(
            60
        )  # wait for 60 seconds and check whether cluster is started or not

    # Once command is executed, fetch the total_rows
    total_rows = get_total_rows(catalog_name, database_name, table_name)
    print(
        f"The total rows of {catalog_name}.{database_name}.{table_name} are: {total_rows}"
    )

    # Fetch the data from the table
    data_df = fetch_table_data(
        catalog_name, database_name, table_name, total_rows, version
    )
    print(data_df)
    print(f"The length of dataframe is: {len(data_df)}")

    # Once data is fetched, then fetch the metadata
    return {
        "table_data": data_df,
        "table_metadata": get_table_metadata(catalog_name, database_name, table_name),
    }


if __name__ == "__main__":
    # catalog_name = input("Enter the catalog name: ")
    # database_name = input("Enter the database name: ")
    # table_name = input("Enter the table name: ")

    # # TODO: Show on terminal that cluster has started, show each step on terminal, now when we don't get command output, then
    # # again send the request to same endpoint to get the output. Do it until we don't get the output
    # data = main(catalog_name, database_name, table_name, 2)
    # print(data["table_data"])
    # print(data["table_metadata"])

    # versions = table_version(database_name, table_name, catalog_name)
    # print(versions)

    # catalogs = fetch_catalogs()
    # print(catalogs)

    catalog_name = "auto_insurance"
    # schemas = fetch_schemas(catalog_name)
    # print(schemas)

    catalog_name = "auto_insurance"
    schema_name = "default"
    # tables = fetch_tables(schema_name, catalog_name)
    # print(tables)

    volume_name = "test"
    storage_location = fetch_volume_storage(catalog_name, schema_name, volume_name)
    print(storage_location)



