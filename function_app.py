import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime

app = func.FunctionApp()  # top-level app

@app.function_name(name="TimerGenerateCsv")
@app.schedule(schedule="*/5 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def timer_generate_csv(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger funkce, která běží každých 5 minut a generuje CSV do Blob Storage.
    """
    try:
        account_url = "https://kiltaine.blob.core.windows.net"
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        container_name = "datove-vystupy"
        blob_name = f"test_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        now = datetime.now()
        data = {
            "data1": ["test1", now],
            "data2": ["test2", now],
            "data3": ["test3", now]
        }

        df = pd.DataFrame(data)
        csv_output = df.to_csv(index=False)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(csv_output, overwrite=True, encoding='utf-8')

        print(f"CSV {blob_name} úspěšně nahrán.")
    except Exception as e:
        print(f"Chyba při nahrávání CSV: {e}")
