from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from io import BytesIO

from fetch_catalog_data import get_catalog_data
import xlsxwriter
import pandas as pd


# Create an instance of the FastAPI class
app = FastAPI()

# Specify the template directory
template = Jinja2Templates(directory="templates")

# Mount the static files (Create url)
app.mount("/static", StaticFiles(directory="static"), name="static")


# Define a route using a decorator
@app.get(
    "/", response_class=HTMLResponse
)  # by default it takes the response class, but it is good practice to mention that
async def main(request: Request):
    return template.TemplateResponse("index.html", {"request": request})


@app.get("/fetch_data/{catalog_name}/{database_name}/{table_name}")
async def fetch_data(
    request: Request, catalog_name: str, database_name: str, table_name: str
):
    print(f"Catalog Name: {catalog_name}")
    print(f"Database Name: {database_name}")
    print(f"Table Name: {table_name}")

    # Fetch data from aws databricks
    try:
        # fetch the catalog data
        data_and_metadata_info = get_catalog_data(
            share_name=catalog_name, schema_name=database_name, table_name=table_name
        )

         # Define XLSX file name
        file_name = f"{table_name}.xlsx"

        # Set response headers to enable file download
        headers = {
            "Content-Disposition": f"attachment; filename={file_name}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

        # Create a BytesIO buffer
        buffer = BytesIO()

        # Use ExcelWriter to write multiple DataFrames to the same Excel file
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            data_and_metadata_info["data"].to_excel(
                writer, sheet_name=f"{table_name} data", index=False
            )
            data_and_metadata_info["metadata"].to_excel(
                writer, sheet_name=f"{table_name} metadata", index=False
            )

        # Return the content of the BytesIO buffer directly
        buffer.seek(0)
        return Response(content=buffer.read(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)
    except Exception as error:
        # Simulate an error condition
        raise HTTPException(status_code=400, detail="This is a custom error message")