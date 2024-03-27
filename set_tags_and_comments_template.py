import fetch_catalog_data as fc
import streamlit as st
import upload_catalog_data as up
import numpy as np


## Only give the file upload access to the users. Don't allow them to create a catalog, database, or volume
def main():
    st.title("Databricks Table Uploader")
    st.info("The comment will be reflected in the current version of table!")

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

    # fetch the list of table names
    tables = fc.fetch_tables(schema_name, catalog_name)

    # Create a dropdown list using st.selectbox
    # Once the schema is selected give the 2 option of tables and volumes
    table_name = st.selectbox(
        "Select a table:", tables, placeholder="Choose an option"
    )
    print(table_name)

    # Example list of columns. Ask user, whether they need to set the comments for 
    # current version or previous version
    table_matadata = fc.get_table_metadata(catalog_name, schema_name, table_name)
    print(table_matadata)

    columns_list = list(table_matadata["name"])
    print(columns_list)

    existing_comment_list = list(table_matadata["metadata.comment"].replace({np.nan: None}))
    print(existing_comment_list)

    column_name = st.selectbox(
        "Select a column to add comment:", columns_list, placeholder="Choose an option"
    )
    
    # Find the index of column
    column_index = columns_list.index(column_name)

    # Based on the index fetch the comment
    existing_comment = existing_comment_list[column_index]
    print(existing_comment)

    if "new_comment" not in st.session_state:
        st.session_state["new_comment"] = None

    if existing_comment:
        # Display existing comment in a disabled text area
        st.text_area("Existing Comment:", value=existing_comment, disabled=True)

        # Button to enable editing
        if st.button("Change Comment"):
            # Enable editing by displaying a text area
            # st.session_state["new_comment"] = 
            st.text_area("Enter new comment:", key="new_comment")
    else:
        # Display existing comment in a disabled text area
        # st.session_state["new_comment"] = 
        st.text_area("Enter new comment:", key="new_comment")

    if "alter_comments" not in st.session_state:
        st.session_state["alter_comments"] = {}

    
    add_comment, remove_button = st.columns(2)

    # Update the comment when the user clicks a separate button
    if add_comment.button("Add Comment"):
        new_comment = st.session_state["new_comment"]
        if new_comment is None or new_comment=="":
            st.error("Please add a comment!")
            st.session_state["new_comment"] = None
            st.stop()
        
        
        st.session_state["alter_comments"][column_name] = new_comment
        st.success("Comment added successfully!")

    if remove_button.button("Remove Comment"):
        st.session_state["alter_comments"].pop(column_name)
    
    # Display the session state
    st.write("Comments to change:", st.session_state["alter_comments"])
    print(st.session_state)

    if st.button("Update the given comments"):
        # Call the method to update comments
        alter_comments_dict = st.session_state["alter_comments"]
        print(alter_comments_dict)

        # Update the comments
        up.update_comments(catalog_name, schema_name, table_name, alter_comments_dict)
        st.success("Comments Updated Successfully!")


if __name__ == "__main__":
    main()
