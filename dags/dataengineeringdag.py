import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task


@dag(schedule='@hourly', start_date=datetime(2025, 5, 25), catchup=False)
def etl():
    @task
    def extract():
        data = {
            "PC": {
                "id": 1,
                "name": "Mine PC",
                "parts": {
                    "cpu": "AMD Ryzen 7 9700X",
                    "gpu": "Gigabyte RTX 5070 Gaming OC",
                    "ram": "Gingston Fury 16GB x2 6000 MT/s",
                    "storage": "Samsung Evo 990 Pro 1TB",
                    "psu": "MSI MAG 850W",
                    "motherboard": "MSI MAG Tomahawk B850 MAX Wi-Fi",
                    "monitor": "Asus ROG 24″ IPS QHD"
                }
            }
        }
        return data

    @task
    def transform(data):
        transformed = {
            "id": data["PC"]["id"],
            "name": data["PC"]["name"],
            "cpu": data["PC"]["parts"]["cpu"],
            "gpu": data["PC"]["parts"]["gpu"],
            "ram": data["PC"]["parts"]["ram"],
            "storage": data["PC"]["parts"]["storage"],
            "psu": data["PC"]["parts"]["psu"],
            "motherboard": data["PC"]["parts"]["motherboard"],
            "monitor": data["PC"]["parts"]["monitor"]
        }

        return transformed

    @task
    def load(data):
        df = pd.DataFrame([data])
        
        print(df)

    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)


etl()