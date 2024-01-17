// function submitForm() {
//     const catalogName = document.getElementById("catalogName").value;
//     const databaseName = document.getElementById("databaseName").value;
//     const tableName = document.getElementById("tableName").value;

//     const apiMessage = document.getElementById("apiMessage");

//     // Send request to /{catalog_name}/{database_name}/{table_name} endpoint
//     fetch(`/${catalogName}/${databaseName}/${tableName}`)
//     .then(response => {
//         // Check if response status is ok
//         if(!response.ok) {
//             throw new Error(`HTTP error! Status: ${response.status}`);
//         }  
//     })
//     .then(data => {
//         // Set the message as Data Successfully downloaded
//         apiMessage.innerText = `✅ The ${tableName} table data successfully fetched!`;

//         // Handle the data from the API
//         console.log(`API Response: ${data}`);
//     })
//     .catch(error => {
//         // Set the message as Data Successfully downloaded
//         apiMessage.innerText = `❌ ${error}!`;

//         // Handle any errors that occurred during the fetch
//         console.error('Error:', error);
//     });
// }


async function submitForm() {

    const catalogName = document.getElementById("catalogName").value;
    const databaseName = document.getElementById("databaseName").value;
    const tableName = document.getElementById("tableName").value;

    const apiMessage = document.getElementById("apiMessage");

    // console.log(catalogName);
    // console.log(databaseName);
    // console.log(tableName);

    if(!catalogName || !databaseName || !tableName) {
        // console.log("Please enter the required fields!");
        apiMessage.innerText = "⚠️ Please enter the required fields!"
        return
    }

    try {
        // Makes asynchronous request to the /fetch_data/catalog_name/.... endpoint
        // The await keyword is used to wait for the promise to resolve, and the resulting Response object is stored in the response variable.
        const response = await fetch(`/fetch_data/${catalogName}/${databaseName}/${tableName}`);
        console.log(response);

        // Check if the response status is ok (status code in the range 200-299)
        if (!response.ok) {
            const errorData = await response.json();  // Assuming the error details are in JSON format
            throw new Error(`HTTP error! Status: ${response.status}, Message: ${errorData.detail}`);
        }

        // This line asynchronously converts the response body into a Blob object using the blob() method. The await keyword is used again to wait for the conversion to complete.
        const blob = await response.blob();
        console.log(blob);

        // This line dynamically creates an anchor (<a>) element in the DOM. This element will be used to trigger the download of the Excel file.
        const link = document.createElement('a');
        console.log(link);

        // the href attribute of the anchor element is set to a URL created from the Blob object. This URL represents the content of the Excel file.
        link.href = window.URL.createObjectURL(blob);
        console.log(link.href);

        // The download attribute of the anchor element is set to suggest the filename for the downloaded file. In this case, it's set to 'example.xlsx'.
        link.download = `${tableName}.xlsx`

        // The anchor element is appended to the document body. This is necessary for the next step, where the click event is triggered.
        document.body.appendChild(link);

        // This line programmatically triggers a click event on the anchor element. This action simulates a user clicking on a link, which prompts the browser to download the file.
        link.click();

        // After the download is triggered, the anchor element is removed from the document body. This step is done to clean up and not leave unnecessary elements in the DOM.
        document.body.removeChild(link);

        // Set the message as Data Successfully downloaded
        apiMessage.innerText = `✅ The ${tableName} table data successfully fetched!`;

    } catch (error) {
        apiMessage.innerText = `❌ Error Downloading ${tableName} table data! Check whether table exists or not.`;
    }
}