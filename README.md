# Pipeline de Datos Financieros: Proyectos BID e Indicadores Económicos

Este proyecto implementa un pipeline de datos E2E (End-to-End) para analizar la relación entre los proyectos de inversión del **Banco Interamericano de Desarrollo (BID)** y los indicadores económicos clave (Desempleo y CPI) a nivel global, facilitando la toma de decisiones financieras y el análisis de riesgo país.

## 🏗️ Arquitectura del Pipeline

El flujo de datos utiliza datos abiertos provenientes de [IADB Data](https://data.iadb.org/dataset/) y otras fuentes económicas:
1.  **Origen de Datos:** Datos abiertos del BID (Proyectos), Desempleo y CPI Index (IPC).
2.  **Ingesta (EL):** [Airbyte Cloud](https://airbyte.com/) sincroniza los datos desde fuentes externas hacia **MotherDuck**.
3.  **Almacenamiento (Data Warehouse):** [MotherDuck](https://motherduck.com/) actúa como el repositorio central operando sobre el catálogo `intdt`.
4.  **Orquestación:** [Prefect 2.x](https://www.prefect.io/) gestiona el flujo modularizado (Ingesta -> Auditoría -> Transformación -> BI).
5.  **Transformación (T):** [dbt (data build tool)](https://www.getdbt.com/) realiza el modelado de datos con DuckDB, aplicando una capa de limpieza robusta contra datos ruidosos.
6.  **Visualización (BI):** [Metabase](https://www.metabase.com/) genera dashboards de riesgo país e inversión con **persistencia de datos** habilitada mediante volúmenes de Docker.

## 🛠️ Herramientas y Tecnologías

| Herramienta | Función |
| :--- | :--- |
| **Docker & Compose** | Contenerización de Prefect, Metabase y dbt. |
| **Prefect** | Orquestación granular de flujos de trabajo. |
| **Airbyte Cloud** | Ingesta desde fuentes externas hacia MotherDuck. |
| **MotherDuck** | Almacenamiento OLAP basado en DuckDB. |
| **dbt-duckdb** | Transformaciones SQL y validación de tipos de datos. |
| **Metabase** | Visualización con base de datos H2 persistente. |

## 🚀 Instalación y Despliegue

1.  **Clonar el repositorio.**
2.  **Configurar el entorno:** Asegúrate de que `MOTHERDUCK_TOKEN` esté disponible en el archivo `.env`.
3.  **Levantar el stack:**
    ```bash
    docker compose up -d --build
    ```
4.  **Ejecutar transformaciones dbt:**
    ```bash
    cd dbt_project && dbt build --profiles-dir .
    ```

5.  **Acceso a las herramientas:**
    - **Prefect UI:** `http://localhost:4200`
    - **Metabase:** `http://localhost:3000` (Configuraciones persistentes en el volumen `metabase-data`)

## 📊 Estructura de Datos (dbt)

El proyecto implementa una arquitectura de modelado en capas para garantizar la calidad del dato:

### 1. Capa de Staging (Limpieza y Normalización)
- **`stg_bid_projects`**: Normaliza proyectos del BID. Extrae `approval_year` de forma robusta y mapea códigos de país ISO2 a ISO3.
- **`stg_cpi`**: Limpia el Índice de Precios al Consumidor. Filtra filas de cabecera (`ISO='ISO'`), normaliza decimales (`,` a `.`) y extrae años de 4 dígitos.
- **`stg_unemployment`**: Procesa la tasa de desempleo con limpieza de tipos y filtrado de ruido textual.
- **`stg_country_mapping`**: Tabla de referencia para unificar códigos de país del BID con estándares internacionales ISO3.

### 2. Capa de Marts (Lógica de Negocio)
- **`dim_countries`**: Dimensión maestra de países única por `country_code` (ISO3).
- **`fact_financial_indicators`**: Unión (Full Outer Join) de CPI y Desempleo por País y Año, asegurando que no se pierdan datos dispersos.
- **`mart_annual_inflation`**: Calcula la inflación porcentual anual utilizando funciones de ventana (`LAG`) sobre el histórico de CPI.
- **`mart_country_financial_risk`**: Modelo central que correlaciona la inversión del BID con el riesgo país (Desempleo e Inflación) y genera un `simple_risk_index`.

## 🛡️ Robustez y Calidad del Dato

Se implementaron múltiples mecanismos para manejar datos ruidosos provenientes de Airbyte/CSV:
- **Materialización como TABLA:** Los modelos de staging se materializan como tablas para romper la inferencia de tipos incorrecta de DuckDB sobre fuentes sucias.
- **Regex Cleaning:** Extracción estricta de años (4 dígitos) para evitar que texto basura rompa las agregaciones temporales.
- **Safe Casting:** Uso de `try_cast` y `replace` para manejar valores `'NULL'`, `'NA'` o formatos de moneda con comas.
- **Normalización de Strings:** Todas las llaves de JOIN se procesan con `upper(trim(cast(... as varchar)))` para evitar fallos por espacios invisibles o diferencias de tipo.

## 📋 Esquemas de Origen (Airbyte)

Para la configuración de las fuentes en Airbyte, se esperan las siguientes estructuras en MotherDuck:

### 🍃 MongoDB (Proyectos BID)
Basado en datos de [IADB Hub](https://data.iadb.org/ecosystem/iadb-project-data).
La ingesta se realiza desde MongoDB hacia MotherDuck.
- **Tabla:** `proyectos_bid`
- **Campos clave:** `oper_num` (ID de proyecto), `cntry_cd` (ISO-2), `totl_cost_orig` (Monto total).
- **Limpieza:** Se filtran cabeceras de texto y se normalizan los separadores de decimales.

### 🔍 BigQuery (Indicadores Económicos)
Datos económicos globales para análisis comparativo.
- **Dataset Desempleo:** `desempleo` (Campos: `country_name`, `iso`, `value`, `period`).
- **Dataset CPI:** `cpi_indice` (Campos: `country_name`, `iso`, `value`, `period`).
- **Nota técnica:** Los valores numéricos se ingieren como `VARCHAR` para asegurar la compatibilidad con el origen y se transforman a `DOUBLE` en dbt.

## 🧪 Pruebas de Calidad (Data Contracts)
El pipeline incluye una etapa obligatoria de auditoría de datos en Prefect que ejecuta:
- **Pruebas Genéricas:** `unique` y `not_null` sobre identificadores de proyectos y países.
- **dbt-expectations:** Validaciones de tipo de columna en el origen para detectar cambios inesperados en el esquema de Airbyte.
- **Limpieza Automática:** Filtrado de registros con basura de datos (ej. filas con texto "valor" en columnas numéricas).

## 📊 Modelos de Datos dbt

| Modelo | Nivel | Descripción |
| :--- | :--- | :--- |
| `stg_bid_projects` | Staging | Limpieza de proyectos BID, normalización de fechas y montos USD. |
| `stg_unemployment` | Staging | Normalización de tasas de desempleo por país/año. |
| `stg_cpi` | Staging | Normalización del Índice de Precios al Consumidor (CPI). |
| `dim_countries` | Marts | Catálogo maestro de países con códigos ISO y nombres normalizados. |
| `fact_financial_indicators` | Marts | Tabla de hechos que une inversiones BID con indicadores económicos. |
| `mart_country_financial_risk` | Reporting | KPI de Riesgo Financiero por país basado en inversión vs volatilidad económica. |

## 🐍 Librerías Python Principales
- `prefect`: Engine de orquestación.
- `dbt-duckdb`: Integración de dbt con DuckDB/MotherDuck.
- `duckdb`: Cliente para ejecución de consultas locales y remotas.
- `requests`: Para llamadas a la API de Airbyte.
