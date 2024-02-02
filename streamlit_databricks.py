import streamlit as st
import pandas as pd
import numpy as np
import fetch_catalog_data as fc
from azure_data_storage import fetch_files, download_file_from_s3
# from s3_methods import get_s3_data, fetch_files, download_file_from_s3
import os

def main():

    st.title("Databricks Table Fetcher")

    # No need of fetching catalogs from API, all the data is going to store in this single catalog
    # catalogs_list = ['content_datasets'] #, 'main', 'system'] #TODO: fetch the catalogs list from api
    catalogs_list = fc.fetch_catalogs()
    
    # Create a dropdown list using st.selectbox
    catalog_name = st.selectbox("Select a catalog:", catalogs_list, placeholder="Choose an option")
    print(catalog_name)

    # Fetch all the tables of that particular catalog via API
    schemas = fc.fetch_schemas(catalog_name)

    # Create a dropdown list using st.selectbox
    schema_name = st.selectbox("Select a database:", schemas, placeholder="Choose an option")
    print(schema_name)

    # Once the schema is selected give the 2 option of tables and volumes
    selected_option = st.radio("Select an option:", ["Tables", "Volumes"])

    if selected_option == "Tables":
        # Display the selected option
        st.write(f"You selected: {selected_option}")

        # fetch the list of table names
        tables = fc.fetch_tables(schema_name, catalog_name)

        # Create a dropdown list using st.selectbox
        table_name = st.selectbox("Select a table:", tables, placeholder="Choose an option")
        print(table_name)

        if table_name:
            # Give the version box to select which version data they want
            table_versions = fc.table_version(schema_name, table_name, catalog_name)
        
            # Create a dropdown list using st.selectbox
            versions = st.selectbox("Select the version:", table_versions, placeholder="Choose an option")

        
        # st.write(f"You selected the table: {table_name}")
        # if st.button("Fetch Data"):
        #     # Call fetch_catalog_data main function to fetch data
        #     data = fc.main(catalog_name, schema_name, table_name, versions)
            
        #     # Display a subset of the data
        #     st.subheader("Table Data:")
        #     st.write(data["table_data"])

    #         st.subheader("Table Metadata:")
    #         st.write(data["table_metadata"])
    elif selected_option == "Volumes":
        # Display the selected option
        st.write(f"You selected: {selected_option}")

        # Get user input for the Delta table name
        volume_name = st.text_input("Enter Volume Name:", "test") # default, we can use any other also
        print(volume_name)

        try:
            storage_location = fc.fetch_volume_storage(catalog_name, schema_name, volume_name)
            print(storage_location)
        except:
            st.write(f"There is no such volume called **{volume_name}** in {catalog_name}.{schema_name}")
            return

        # Once we got the location, let's fetch the data from that
        total_files = fetch_files(storage_location)
        print(total_files)
        
        # Create a drop down of available files
        file = st.selectbox("Select the file:", total_files, placeholder="Choose an option")
        
        default_download_path = os.path.expanduser(r"~\Downloads")
        local_path = os.path.join(default_download_path, file)
        print(file)

    #     # Example usage
    #     object_key = f"{key}/{total_files[0]}"  # Specify the object key of the file in S3
    #     default_download_path = os.path.expanduser(r"~\Downloads")
    #     local_path = os.path.join(default_download_path, file)
    #     # local_path = f"C:/Users/burha/Downloads/{total_files[0]}"  # Specify the local path to save the downloaded file
    #     print(bucket_name)
    #     print(object_key)

        # Download the file from S3
        if st.button("Fetch Data"):
            # download_file_from_s3(bucket_name, object_key, local_path)
            download_file_from_s3(storage_location, file, local_path)

            st.write(f"The file successfully downloaded at: **{local_path}**")


# ___________________________________________________________________________________    
    # catalog_name = st.text_input("Enter the catalog name:")
    # database_name = st.text_input("Enter the database name:")
    # table_name = st.text_input("Enter the table name:")

    # if st.button("Fetch Data"):
    #     # Call your main function to fetch data
    #     data = fc.main(catalog_name, database_name, table_name)
        
    #     # Display a subset of the data
    #     st.subheader("Table Data:")
    #     st.write(data["table_data"])

    #     st.subheader("Table Metadata:")
    #     st.write(data["table_metadata"])


# def fetch_data(catalog_name, database_name, table_name):
#     # Replace 'main' with your actual main function
#     # return main(catalog_name, database_name, table_name)
#     # Creating a DataFrame with 500 records
#     data = {
#         'ID': range(1, 500001),
#         'Name': [f'Person_{i}' for i in range(1, 500001)],
#         'Age': np.random.randint(18, 60, size=500000),
#         'Salary': np.random.uniform(30000, 80000, size=500000)
#     }

#     df = pd.DataFrame(data)

#     # Display the DataFrame
#     return {
#         "table_data": df
#     }


if __name__ == "__main__":
    main()
