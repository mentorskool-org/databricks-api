import streamlit as st

st.set_page_config(
    page_title="Unity_Catalog_Guide",
    page_icon="👋",
)

st.write(
    """
         <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"> 

         # Unity Catalog Template! <i class="fa-solid fa-database"></i>
         """,
    unsafe_allow_html=True,
)

st.sidebar.success("Select a tab from above!")

st.markdown(
    """
    ## Notes:
    - **Upload Data Template** will help you to Upload the data from local to Databricks Unity Catalog
    - **Download Catalog Data** Template will help you to Download the data from Databricks Unity Catalog
    - **Set Tags and Comments** Template will allow you to set the comments for different columns of a Unity Catalog Table. 
    This comments will be stored in the metadata, and will help you to understand the data.

    ## Feature Need to be Added:
    - The current functionality of Set Tags and Comments only enables users to add comments. 
      The capability to include tags is currently under development and will be integrated soon
      """
)
