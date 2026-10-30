import random
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Asset

import pandas as pd

RUTA = "/opt/airflow/data/clima.csv"
CLIMA = Asset(f"file://{RUTA}")

@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 23),
    catchup=True,
    tags=["ejercicio e6", "productor"],
)

def e6_producto():
    
    @task
    def listar_ciudades()-> list[str]:
        return ["lima", "quito", "bogota", "santiago"]
    
    @task
    def medir(ciudad: str, ds=None) -> dict[str, int | float]:
        temp = random.Random(f"{ciudad} - {ds}").uniform(8, 30)
        return {
            "ciudad": ciudad,
            "temp": round(temp, 1)
        }
        
    @task(outlets=[CLIMA])
    def guardar(medidas: list[dict]):
        Path(RUTA).parent.mkdir(parents=True, exist_ok=True)
        df= pd.DataFrame(list(medidas), columns=['ciudad', 'temp'])
        df.to_csv(RUTA, index=False)
        
    guardar(medir.expand(ciudad=listar_ciudades()))
    
@dag(
    schedule=[CLIMA],
    start_date=datetime(2026, 10, 27),
    catchup=True,
    tags=["Ejercicio 6", "consumidor"]
)

def e6_consumidor():
    
    @task
    def informe():
        df = pd.read_csv(RUTA)
        #top = df.loc[df['temp'].idxmax()]
        print(f"data: {df}")
        
    informe()
    
e6_producto()
e6_consumidor()
    
