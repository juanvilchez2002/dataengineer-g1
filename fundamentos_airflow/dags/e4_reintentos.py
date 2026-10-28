from datetime import datetime, timedelta

from airflow.sdk import dag,task

@dag(
    schedule=None,
    start_date=datetime(2026, 10,27),
    catchup=False,
    tags=["ejercicio e4", "reintentos"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10)
    }
)

def e4_reintentos():
    
    # la variable ti es una palabra reservada de airflow
    @task
    def api_inestable(ti=None)-> str:
        print(f"Número de reintentos: {ti.try_number}")
        
        if ti.try_number < 3:
            raise RuntimeError("La API no responde")
        
        return "Los datos estan OK"
    
    
    @task
    def procesar(datos: str):
        print(f"Procesando datos: {datos}")
    
    
    # all_done -> si todos han terminado sin saber si es exito o fallido
    @task(trigger_rule="all_done")
    def notificar():
        print("El pipeline terminó (con éxito o con fallos): revisar.")
        
    procesar(api_inestable()) >> notificar()
    
e4_reintentos()
        