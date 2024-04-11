import fetch_catalog_data as fc
import streamlit as st
from upload_catalog_data import upload_file_to_volume, upload_file_to_table


## Only give the file upload access to the users. Don't allow them to create a catalog, database, or volume
def main():
    # st.title("Databricks Table Uploader")

    st.write(
    """
         <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"> 

         # Databricks Table Uploader! <i class="fa-solid fa-upload"></i>
         """,
    unsafe_allow_html=True,
)

    # No need of fetching catalogs from API, all the data is going to store in this single catalog
    # catalogs_list = ['content_datasets'] #, 'main', 'system'] #TODO: fetch the catalogs list from api
    catalogs_list = fc.fetch_catalogs()

    # Create a dropdown list using st.selectbox
    catalog_name = st.selectbox(
        "Select a catalog:", catalogs_list, placeholder="Choose an option"
    )
    print(catalog_name)

    # Fetch all the tables of that particular catalog via API
    schemas = fc.fetch_schemas(catalog_name)

    # Create a dropdown list using st.selectbox
    schema_name = st.selectbox(
        "Select a database:", schemas, placeholder="Choose an option"
    )
    print(schema_name)

    # Once the schema is selected give the 2 option of tables and volumes
    selected_option = st.radio("Upload the data as:", ["Table", "Volume"])

    if selected_option == "Volume":
        # Ask to browse the file
        st.write("Volume is selected")

        # fetch the list of table names
        volumes = fc.fetch_volumes(schema_name, catalog_name)
        print(volumes)

        # volume_name = st.text_input(
        #     "Enter Volume Name:", "test"
        # )  # default, we can use any other also
        # volume_name = st.selectbox(
        #     "Select the volume: ", volumes, placeholder="Choose an option"
        # )
        # print(volume_name)

        selected_option = st.radio(
            "Want to upload the data in the existing volume:",
            ["Yes", "No, create a new volume"],
        )
        print(selected_option)

        if selected_option == "Yes":
            volume_name = st.selectbox(
                "Select a volume:", volumes, placeholder="Choose an option"
            )
            print(volume_name)

            storage_location = fc.fetch_volume_storage(
                catalog_name, schema_name, volume_name
            )
            print(storage_location)

            new_volume = False
        else:
            # Get user input for the Delta table name
            volume_name = st.text_input("Enter the name of the volume: ")

            # Here check one more thing, that if table_name already exists is the table, then ask them to
            # select it from dropdown
            if volume_name in volumes:
                st.warning(
                    f"""The volume {volume_name} already exists in the database. if you want to overwrite the file in the volume 
                    then select the volume from the dropdown."""
                )
                st.stop()

            # Create the volume
            new_volume = True

        file_to_upload = st.text_input(
            "Enter the file path from local with extension to upload:"
        )  # default, we can use any other also
        print(file_to_upload)

        # Download the file from S3
        if st.button("Upload Data"):
            with st.spinner(text="In progress...", cache=False):
                if new_volume:
                    try:
                        response = fc.create_volume(
                            catalog_name, schema_name, volume_name
                        )
                        storage_location = response["storage_location"]
                    except Exception as error:
                        st.error(error)
                        st.stop()

                # Pass this file to upload on volume
                message = upload_file_to_volume(storage_location, file_to_upload)
                st.write(message + f" Path: {catalog_name}.{schema_name}.{volume_name}")
    elif selected_option == "Table":
        # Display the selected option
        st.write(f"You selected: {selected_option}")

        # fetch the list of table names
        tables = fc.fetch_tables(schema_name, catalog_name)

        # Create a dropdown list using st.selectbox
        # Once the schema is selected give the 2 option of tables and volumes
        selected_option = st.radio(
            "Want to upload the data in the existing table:",
            ["Yes", "No, create a new table"],
        )
        print(selected_option)

        if selected_option == "Yes":
            table_name = st.selectbox(
                "Select a table:", tables, placeholder="Choose an option"
            )
            print(table_name)
        else:
            # Get user input for the Delta table name
            table_name = st.text_input("Enter the name of the table: ")

            # Here check one more thing, that if table_name already exists is the table, then ask them to
            # select it from dropdown
            if table_name in tables:
                st.warning(
                    f"The table {table_name} already exists in the database. if you want to overwrite the data then select it from the dropdown."
                )
                st.stop()

        file_to_upload_location = st.text_input(
            "Enter the file path that you have to upload:"
        )  # default, we can use any other also
        print(file_to_upload_location)

        description = st.text_area("Enter the description for this operation!")
        print(f"Description: {description}")

        # Download the file from S3
        if st.button("Upload File as Table"):
            if description=="":
                st.error("Please add the description")
                st.stop()
 
            try:
                with st.spinner("Processing..."):
                    response = upload_file_to_table(
                        catalog_name, schema_name, table_name, file_to_upload_location, description
                    )
                    
                    # Once file is uploaded fetch the version of that file
                    try:
                        versions_index, version_description_map, cort_versions, ccol_versions, all_versions = fc.table_history(
                            schema_name, table_name, catalog_name
                        )
                        latest_version = versions_index[-1]
                    except Exception as error:
                        print(error)
                        # if "TABLE_OR_VIEW_NOT_FOUND" in str(error):
                        #     latest_version = 0

                    print(latest_version)
                    if response:
                        # we are adding 1 to table_versions because although the table gets updated,
                        # it gets time to reflect in version, thus the previous version is shown
                        # that's why adding 1 in previous version will be latest version
                        st.success(
                            f"""The data has been effectively uploaded into the {table_name} table 
                            within the {schema_name} schema of the {catalog_name} catalog!\n
                            Version {latest_version} of table {table_name} is created."""
                        )
                    else:
                        st.error(
                            "The file is not csv, please give the path of another file!"
                        )
            except FileNotFoundError as error:
                print(error)
                st.error(error)
            except Exception as error:
                print(error)
                st.error(error)


if __name__ == "__main__":
    main()
