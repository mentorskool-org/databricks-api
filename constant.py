from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())


# AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")

DATABRICKS_SERVER_HOSTNAME = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH")

# Initialize the connection string (you can find this in your Azure portal)
AZURE_CONNECTION_STRING = os.environ.get("AZURE_CONNECTION_STRING")

CLUSTER_ID = os.environ.get("CLUSTER_ID")