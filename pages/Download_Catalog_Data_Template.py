import streamlit as st
import fetch_catalog_data as fc
from azure_data_storage import fetch_files, download_file_from_s3
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

    # remove main from catalog
    catalogs_list.remove("main")

    # Create a dropdown list using st.selectbox
    catalog_name = st.selectbox(
        "Select a catalog:", catalogs_list, placeholder="Choose an option"
    )

    # Fetch all the tables of that particular catalog via API
    schemas = fc.fetch_schemas(catalog_name)

    # Ignore the default and information_schema
    schemas.remove("default")
    schemas.remove("information_schema")

    # Create a dropdown list using st.selectbox
    schema_name = st.selectbox(
        "Select a database:", schemas, placeholder="Choose an option"
    )

    # Once the schema is selected give the 2 option of tables and volumes
    selected_option = st.radio("Select an option:", ["Tables", "Volumes"])

    if selected_option == "Tables":
        # Display the selected option
        print(f"You selected: {selected_option}")

        # fetch the list of table names
        tables = fc.fetch_tables(schema_name, catalog_name)

        # Create a dropdown list using st.selectbox
        table_name = st.selectbox(
            "Select a table:", tables, placeholder="Choose an option"
        )

        if table_name:
            # Give the version box to select which version data they want
            (
                versions_index,
                versions_map_description,
                cort_versions,
                ccol_versions,
                all_versions,
            ) = fc.table_history(schema_name, table_name, catalog_name)

            # Create a dropdown list using st.selectbox
            versions = list(versions_map_description.keys())[::-1]
            selected_version_index = st.selectbox(
                "Select the version:",
                versions_index[::-1],
                placeholder="Choose an option",
            )
            version = versions[selected_version_index]
            st.info(f"Description: {versions_map_description[version]}")

            preview_data, fetch_data = st.columns(2)

            if preview_data.button("Preview Data"):
                # Display spinner while the process is ongoing
                with st.spinner("Processing..."):
                    table_data_df = fc.fetch_data_via_databricks_connector(
                        catalog_name, schema_name, table_name, version, preview=True
                    )

                # Display a subset of the data
                st.subheader(f"Preview of the {table_name} Data:")
                st.write(table_data_df)

            # st.write(f"You selected the table: {table_name}")
            if fetch_data.button("Fetch Data"):
                # Display spinner while the process is ongoing
                with st.spinner("Processing..."):
                    # Call fetch_catalog_data main function to fetch data
                    # data = fc.main(catalog_name, schema_name, table_name, version)
                    table_data_df = fc.fetch_data_via_databricks_connector(
                        catalog_name, schema_name, table_name, version
                    )

                if table_data_df.empty:
                    st.error(
                        "Problem occured while downloading the data, might be due to invalid data or any other reason. Please contact your team for the same!!"
                    )
                    st.stop()

                default_download_path = os.path.expanduser(r"~\Downloads")
                file_path = os.path.join(
                    default_download_path, f"{table_name}_{selected_version_index}.csv"
                )
                table_data_df.to_csv(file_path, index=False)

                st.info(f"The data is successfully downloaded at {file_path}!!")

            if st.button("Fetch Metadata"):
                # First fetch the metadata_version
                meta_version = fc.select_ccol_version(
                    version, cort_versions, ccol_versions
                )

                # Display spinner while the process is ongoing
                with st.spinner("Processing..."):
                    # Call fetch_catalog_data main function to fetch data
                    meta_data = fc.get_table_metadata(
                        catalog_name, schema_name, table_name, meta_version, versions[0]
                    )

                st.subheader("Table Metadata:")
                st.write(meta_data)
        else:
            st.info(
                f"There are no tables available in the {schema_name} schema of the {catalog_name} catalog!!"
            )
    elif selected_option == "Volumes":
        # Display the selected option
        print(f"You selected: {selected_option}")

        # Get user input for the Delta table name
        # volume_name = st.text_input("Enter Volume Name:", "test") # default, we can use any other also
        volumes = fc.fetch_volumes(schema_name, catalog_name)
        volume_name = st.selectbox(
            "Enter Volume Name:", volumes, placeholder="Choose an option"
        )

        try:
            storage_location = fc.fetch_volume_storage(
                catalog_name, schema_name, volume_name
            )
        except:
            st.info(
                # f"There is no such volume called **{volume_name}** in {catalog_name}.{schema_name}"
                "No volumes available!!"
            )
            return

        # Once we got the location, let's fetch the data from that
        total_files = fetch_files(storage_location)

        # Create a drop down of available files
        file = st.selectbox(
            "Select the file:", total_files, placeholder="Choose an option"
        )

        default_download_path = os.path.expanduser(r"~\Downloads")
        local_path = os.path.join(default_download_path, file)

        # Download the file from ADLS
        if st.button("Fetch Data"):
            # download_file_from_s3(bucket_name, object_key, local_path)
            download_file_from_s3(storage_location, file, local_path)

            st.success(f"The file successfully downloaded at: **{local_path}**")


if __name__ == "__main__":
    main()