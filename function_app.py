import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)  # MUSÍ být na top-level 1



@app.function_name(name="GenerateCsv")
@app.route(route="http_trigger")
def GenerateCsv(req: func.HttpRequest) -> func.HttpResponse:
    try:
        account_url = "https://kiltaine.blob.core.windows.net"
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        container_name = "datove-vystupy"
        blob_name = "test_data.csv"
        now = datetime.now()

        data = {"data1":["test1", now],
                "data2":["test2", now],
                "data3":["test3", now]}

        df = pd.DataFrame(data)
        csv_output = df.to_csv()
        blob_path = blob_service_client.get_blob_client(container=container_name,blob=blob_name)
        blob_path.upload_blob(csv_output, overwrite=True, encoding='utf-8')
           


        return func.HttpResponse("Soubor nahrán", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Chyba: {e}", status_code=500)