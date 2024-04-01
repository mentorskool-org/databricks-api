import streamlit as st
import fetch_catalog_data as fc
from azure_data_storage import fetch_files, download_file_from_s3
# from s3_methods import get_s3_data, fetch_files, download_file_from_s3
import os

def main():

    # st.title("Databricks Table Fetcher")

    st.write(
    """
         <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"> 

         # Databricks Table Fetcher! <i class="fa-solid fa-download"></i>
         """,
    unsafe_allow_html=True,
)

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
            table_data = fc.table_history(schema_name, table_name, catalog_name)
        
            # Create a dropdown list using st.selectbox
            versions = list(table_data.keys())
            version = st.selectbox("Select the version:", versions, placeholder="Choose an option")

        # print(table_data)
        print(versions)
        print(version)
        print(table_data[version])
        st.info(f"Description: {table_data[version]}")
        
        # st.write(f"You selected the table: {table_name}")
        if st.button("Fetch Data"):
            # Display spinner while the process is ongoing
            with st.spinner("Processing..."):
                # Call fetch_catalog_data main function to fetch data
                data = fc.main(catalog_name, schema_name, table_name, version)
                
            # Display a subset of the data
            st.subheader("Table Data:")
            st.write(data)
        
        if st.button("Fetch Metadata"):
            # Display spinner while the process is ongoing
            with st.spinner("Processing..."):
                # Call fetch_catalog_data main function to fetch data
                meta_data = fc.get_table_metadata(catalog_name, schema_name, table_name, version, versions[0])

            st.subheader("Table Metadata:")
            st.write(meta_data)
    elif selected_option == "Volumes":
        # Display the selected option
        st.write(f"You selected: {selected_option}")

        # Get user input for the Delta table name
        # volume_name = st.text_input("Enter Volume Name:", "test") # default, we can use any other also
        volumes = fc.fetch_volumes(schema_name, catalog_name)
        volume_name = st.selectbox("Enter Volume Name:", volumes, placeholder="Choose an option")
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

        # Download the file from ADLS
        if st.button("Fetch Data"):
            # download_file_from_s3(bucket_name, object_key, local_path)
            download_file_from_s3(storage_location, file, local_path)

            st.success(f"The file successfully downloaded at: **{local_path}**")


if __name__ == "__main__":
    main()