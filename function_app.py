import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
from datetime import datetime

app = func.FunctionApp()

@app.function_name(name="TimerGenerateCsv")
@app.schedule(schedule="*/5 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def timer_generate_csv(mytimer: func.TimerRequest) -> None:
    try:
        # Connection string přímo z app settings
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

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
        blob_client.upload_blob(csv_output, overwrite=True)

        print(f"CSV {blob_name} úspěšně nahrán.")
    except Exception as e:
        print(f"Chyba při nahrávání CSV: {e}")

