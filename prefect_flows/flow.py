import os
import requests
import time
import subprocess
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

# Detecta si estamos en Docker o local
if os.path.exists("/usr/app"):
    base_dir = "/usr/app"
else:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

load_dotenv(dotenv_path=os.path.join(base_dir, ".env"))

AIRBYTE_API_URL = "https://api.airbyte.com/v1" # URL de la cloud de Airbyte

# --- Configuración (Valores extraídos de .env) ---
AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
# Nuevas conexiones
AIRBYTE_MONGO_MD_CONN_ID = os.getenv("AIRBYTE_MONGO_MD_CONN_ID")
AIRBYTE_BQ_MD_CONN_ID = os.getenv("AIRBYTE_BQ_MD_CONN_ID")

METABASE_URL=os.getenv("METABASE_URL")

METABASE_DASHBOARD_ID=os.getenv("METABASE_DASHBOARD_ID")
METABASE_SESSION_TOKEN = os.getenv("METABASE_SESSION_TOKEN")

# --- Tarea 0: Obtener Token de Acceso (OAuth2) para Airbyte ---
@task(retries=2, retry_delay_seconds=10)
def get_airbyte_access_token():
    logger = get_run_logger()
    logger.info("Solicitando token de acceso a Airbyte via OAuth2...")
    
    if not AIRBYTE_CLIENT_ID or not AIRBYTE_CLIENT_SECRET:
        raise ValueError("Faltan AIRBYTE_CLIENT_ID o AIRBYTE_CLIENT_SECRET en el .env")

    auth_url = "https://api.airbyte.com/v1/applications/token"
    payload = {
        "client_id": AIRBYTE_CLIENT_ID,
        "client_secret": AIRBYTE_CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/json"}
    
    resp = requests.post(auth_url, json=payload, headers=headers)
    resp.raise_for_status()
    
    token = resp.json().get("access_token")
    if not token:
        raise Exception("No se recibió access_token en la respuesta de Airbyte")
        
    logger.info("Token de acceso obtenido correctamente.")
    return token

# --- Tarea 1: Activar sincronización en Airbyte Cloud ---
@task(retries=3, retry_delay_seconds=60)
def trigger_airbyte_sync(connection_id: str, access_token: str):
    logger = get_run_logger()
    logger.info(f"Iniciando sincronización de Airbyte para la conexión {connection_id}...")
    
    headers = {
        "Authorization": f"Bearer {access_token.strip()}",
        "Content-Type": "application/json"
    }
    
    # Iniciar la sincronización
    resp = requests.post(
        f"{AIRBYTE_API_URL}/jobs",
        headers=headers,
        json={"connectionId": connection_id, "jobType": "sync"}
    )
    resp.raise_for_status()
    job_id = resp.json()["jobId"]
    logger.info(f"Sincronización iniciada. ID del trabajo: {job_id}")

    # Consultar estado hasta que sea 'succeeded'
    while True:
        job_status_resp = requests.get(f"{AIRBYTE_API_URL}/jobs/{job_id}", headers=headers)
        status = job_status_resp.json()["status"]
        logger.info(f"Estado actual de sincronización: {status}")
        
        if status == "succeeded":
            break
        elif status in ["failed", "cancelled"]:
            raise Exception(f"La sincronización de Airbyte ha fallado o ha sido cancelada ({status}). ID del trabajo: {job_id}")
        
        time.sleep(30) # Consultar cada 30 segundos
    
    logger.info(f"Sincronización de Airbyte completada con éxito para la conexión {connection_id}")

# --- Tarea 2: Ejecutar construcción de dbt ---
@task
def run_dbt_build():
    logger = get_run_logger()
    logger.info("Instalando dependencias de dbt y ejecutando build...")
    
    dbt_project_path = "/usr/app/dbt_project"
    
    commands = [
        f"cd {dbt_project_path} && dbt deps --profiles-dir .",
        f"cd {dbt_project_path} && dbt build --profiles-dir ."
    ]
    
    for cmd in commands:
        logger.info(f"Ejecutando: {cmd}")
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error(f"Error en comando dbt build: {cmd}\nSalida: {res.stdout}\nError: {res.stderr}")
            raise Exception(f"Fallo en ejecución de dbt build: {cmd}")
        logger.info(f"Éxito build: {res.stdout}")

# --- Tarea 3: Ejecutar pruebas de calidad dbt (dbt-expectations) ---
@task
def run_dbt_quality_tests():
    logger = get_run_logger()
    logger.info("Iniciando pruebas de calidad de datos (dbt-expectations)...")
    
    dbt_project_path = "/usr/app/dbt_project"
    
    # Aseguramos deps por si se corre de forma independiente
    # Luego ejecutamos los tests específicamente de las fuentes
    commands = [
        f"cd {dbt_project_path} && dbt deps --profiles-dir .",
        f"cd {dbt_project_path} && dbt test --select source:airbyte --profiles-dir ."
    ]
    
    for cmd in commands:
        logger.info(f"Ejecutando: {cmd}")
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if res.returncode != 0:
            logger.error(f"Error en pruebas de calidad: {cmd}\nSalida: {res.stdout}\nError: {res.stderr}")
            raise Exception(f"Fallo en los tests de calidad: {cmd}")
        logger.info(f"Éxito tests: {res.stdout}")

# --- Tarea 4: Actualizar Dashboard de Metabase (Visualización) ---
@task(retries=2)
def refresh_metabase():
    logger = get_run_logger()
    logger.info("Actualizando dashboard de Metabase a través de la API...")
    
    mb_user = os.getenv("METABASE_ADMIN_EMAIL")
    mb_pass = os.getenv("METABASE_ADMIN_PASSWORD")

    if not mb_user or not mb_pass:
      logger.warning("Faltan credenciales de Metabase (EMAIL/PASS). Saltando actualización.")
      return

    # 1. Obtener Token de Sesión fresco
    try:
        session_resp = requests.post(
            f"{METABASE_URL}/api/session",
            json={"username": mb_user, "password": mb_pass},
            timeout=10
        )
        session_resp.raise_for_status()
        session_token = session_resp.json()["id"]
    except Exception as e:
        logger.error(f"Error al autenticar en Metabase: {str(e)}")
        return

    # 2. Sincronizar esquema de la base de datos
    headers = {"X-Metabase-Session": session_token}
    
    # El ID de la base de datos suele ser 2 o el configurado en MOTHERDUCK
    db_id = os.getenv("METABASE_DATABASE_ID", "2")
    resp = requests.post(f"{METABASE_URL}/api/database/{db_id}/sync_schema", headers=headers)
    
    if resp.status_code == 200:
        logger.info(f"Sincronización de la DB {db_id} de Metabase activada con éxito.")
    else:
        logger.error(f"Falla en sincronización de Metabase: {resp.status_code} - {resp.text}")

# --- Flujo principal (Flow): Orquestación completa E2E ---
@flow(name="# Pipeline Completo: Importación, Calidad, Transformación y Visualización")
def financial_pipeline():
    logger = get_run_logger()
    logger.info("Iniciando orquestación completa del pipeline...")

    # Paso 1: Ingesta modularizada
    airbyte_import_mongo_flow()
    airbyte_import_bq_flow()
    
    # Paso 2: Auditoría de Calidad
    dbt_quality_flow()

    # Paso 3: Transformación
    dbt_build_flow()

    # Paso 4: Visualización
    metabase_refresh_flow()

    logger.info("Pipeline Completo finalizado!")

# --- Flujos Independientes ---

@flow(name="Airbyte: Importar MongoDB")
def airbyte_import_mongo_flow():
    logger = get_run_logger()
    logger.info("Iniciando importación MongoDB...")
    access_token = get_airbyte_access_token()

    # Usamos la conexión de BID para MongoDB
    trigger_airbyte_sync(AIRBYTE_MONGO_MD_CONN_ID, access_token) 

@flow(name="Airbyte: Importar BigQuery")
def airbyte_import_bq_flow():
    logger = get_run_logger()
    logger.info("Iniciando importación BigQuery...")
    access_token = get_airbyte_access_token()

    # Usamos la conexión de Desempleo/CPI para BQ
    trigger_airbyte_sync(AIRBYTE_BQ_MD_CONN_ID, access_token)

@flow(name="DBT: Ejecutar Transformacion")
def dbt_build_flow():
    logger = get_run_logger()
    logger.info("Iniciando transformación de datos (dbt build)...")
    run_dbt_build()

@flow(name="DBT: Auditoria de Calidad")
def dbt_quality_flow():
    logger = get_run_logger()
    logger.info("Iniciando auditoría de calidad de datos (dbt test)...")
    run_dbt_quality_tests()

@flow(name="Metabase: Actualizar Visualizacion")
def metabase_refresh_flow():
    logger = get_run_logger()
    logger.info("Iniciando actualización de Metabase...")
    refresh_metabase()

if __name__ == "__main__":
    financial_pipeline()


