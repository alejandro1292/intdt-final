# Pipeline de Datos: Impacto de la IA en Empleos

Este proyecto implementa un pipeline de datos E2E (End-to-End) para analizar el impacto de la Inteligencia Artificial en diversas industrias y roles laborales a nivel global.

## 🏗️ Arquitectura del Pipeline

El flujo de datos sigue el siguiente recorrido:
1.  **Origen de Datos:** API de Airbyte Cloud (conectada a fuentes externas).
2.  **Ingesta (EL):** [Airbyte Cloud](https://airbyte.com/) sincroniza los datos directamente hacia **MotherDuck**.
3.  **Almacenamiento (Data Warehouse):** [MotherDuck](https://motherduck.com/) (DuckDB en la nube) actúa como el repositorio central operando sobre el catálogo `intdt`.
4.  **Orquestación:** [Prefect 2.x](https://www.prefect.io/) gestiona el flujo de trabajo, disparando la sincronización de Airbyte y las transformaciones de dbt.
5.  **Transformación (T):** [dbt (data build tool)](https://www.getdbt.com/) con el adaptador `dbt-duckdb` realiza el modelado de datos (Staging -> Marts -> OBT).
6.  **Visualización (BI):** [Metabase](https://www.metabase.com/) se conecta a MotherDuck para generar dashboards interactivos.

## 🛠️ Herramientas y Tecnologías

| Herramienta | Función |
| :--- | :--- |
| **Docker & Compose** | Contenerización de Prefect, Metabase y dbt. |
| **Prefect** | Orquestación de tareas y monitoreo. |
| **Airbyte Cloud** | Herramienta No-Code para ingesta de datos. |
| **MotherDuck** | Motor SQL OLAP optimizado para DuckDB. |
| **dbt-duckdb** | Transformaciones SQL modulares directamente en MotherDuck. |
| **Metabase** | Visualización de datos y explorador SQL. |
| **Portainer (Opcional)** | GUI para gestión de contenedores Docker (especialmente útil en Linux/macOS). |

## 📋 Requisitos Previos

- Docker y Docker Compose instalados.
- Cuenta en **Airbyte Cloud** con un "Connection ID" configurado.
- Cuenta en **MotherDuck** y un "Service Token".
- Variables de entorno configuradas en un archivo `.env` o en el entorno del sistema.

## 🔐 Variables de Entorno

El sistema requiere las siguientes variables:

```bash
# Autenticación Airbyte (OAuth2)
AIRBYTE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AIRBYTE_CLIENT_SECRET=Secret_De_Airbyte

# IDs de Conexión Airbyte (deben coincidir con los pipelines de Cloud)
AIRBYTE_MONGO_MD_CONN_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AIRBYTE_BQ_MD_CONN_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# MotherDuck / dbt
MOTHERDUCK_TOKEN=Token_De_MotherDuck_Aqui
MOTHERDUCK_DATABASE=intdt

# Metabase
METABASE_URL=http://localhost:3000
METABASE_ADMIN_EMAIL=admin@ejemplo.com
METABASE_ADMIN_PASSWORD=Password_Segura_123

# Prefect
PREFECT_API_URL=http://localhost:4200/api
```

## 🚀 Instalación y Despliegue

1.  **Clonar el repositorio.**
2.  **Configurar el entorno:** Asegúrate de que `MOTHERDUCK_TOKEN` esté disponible para el contenedor de dbt y Metabase.
3.  **Levantar el stack:**
    ```bash
    docker compose up -d --build
    ```
4.  **Portainer (Opcional - RECOMENDADO en Linux/macOS):**
    Si deseas gestionar tus contenedores visualmente:
    ```bash
    docker volume create portainer_data
    docker run -d -p 8000:8000 -p 9443:9443 --name portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ce:2.21.0
    ```
    Accede en: `https://localhost:9443`

5.  **Acceso a las herramientas:**
    - **Prefect UI:** `http://localhost:4200`
    - **Metabase:** `http://localhost:3001`

## 📊 Estructura de Datos (dbt)

Los modelos se organizan en:
-   **Staging:** Limpieza inicial y renombrado de columnas desde la tabla `intdt.main.data`.
-   **Marts:** Dimensiones (`dim_jobs`, `dim_locations`) y hechos (`fact_ai_impact`).
-   **OBT (One Big Table):** Tabla final `obt_ai_impact` optimizada para el consumo en Metabase.
## 📋 Esquemas de Origen (Airbyte)

Para la configuración de las fuentes en Airbyte, se esperan las siguientes estructuras en MotherDuck:

### 🍃 MongoDB (Data Principal)
```sql
CREATE TABLE "data"(
  _id VARCHAR,
  "year" DECIMAL(38,9),
  job_id DECIMAL(38,9),
  country VARCHAR,
  industry VARCHAR,
  job_role VARCHAR,
  _ab_cdc_cursor BIGINT,
  skill_gap_index DECIMAL(38,9),
  salary_after_usd DECIMAL(38,9),
  ai_adoption_level DECIMAL(38,9),
  salary_before_usd DECIMAL(38,9),
  _ab_cdc_deleted_at VARCHAR,
  _ab_cdc_updated_at VARCHAR,
  ai_replacement_score DECIMAL(38,9),
  salary_change_percent DECIMAL(38,9),
  wage_volatility_index DECIMAL(38,9),
  ai_disruption_intensity DECIMAL(38,9),
  automation_risk_percent DECIMAL(38,9),
  automation_risk_category VARCHAR,
  remote_feasibility_score DECIMAL(38,9),
  reskilling_urgency_score DECIMAL(38,9),
  skill_transition_pressure DECIMAL(38,9),
  education_requirement_level DECIMAL(38,9),
  skill_demand_growth_percent DECIMAL(38,9),
  _airbyte_raw_id VARCHAR,
  _airbyte_extracted_at TIMESTAMP,
  _airbyte_meta JSON
);
```

### 🔍 BigQuery (Datos LLM)
```sql
CREATE TABLE llm(
  _id VARCHAR,
  arch VARCHAR,
  model VARCHAR,
  notes VARCHAR,
  ratio VARCHAR,
  tokens VARCHAR,
  alscore VARCHAR,
  comapany VARCHAR,
  parameters VARCHAR,
  playground VARCHAR,
  release_date VARCHAR,
  training_dataset VARCHAR,
  _airbyte_raw_id VARCHAR,
  _airbyte_extracted_at TIMESTAMP,
  _airbyte_meta JSON
);
```
## 🐍 Librerías Python Principales
- `prefect`: Engine de orquestación.
- `dbt-duckdb`: Integración de dbt con DuckDB/MotherDuck.
- `duckdb`: Cliente para ejecución de consultas locales y remotas.
- `requests`: Para llamadas a la API de Airbyte.
