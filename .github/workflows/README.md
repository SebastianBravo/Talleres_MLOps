# GitHub Actions — Workflows de CI/CD

## Workflows disponibles

| Workflow | Componente | Imágenes publicadas |
|---|---|---|
| `build-api.yml` | FastAPI inference API | `<user>/mlops-api` |
| `build-streamlit.yml` | Interfaz Streamlit | `<user>/mlops-streamlit` |
| `build-airflow.yml` | Airflow — 2 variantes | `<user>/mlops-airflow` · `<user>/mlops-airflow-compose` |
| `build-mlflow.yml` | MLflow tracking server | `<user>/mlops-mlflow` |
| `build-observability.yml` | Prometheus · Grafana · Locust | `<user>/mlops-prometheus` · `<user>/mlops-grafana` · `<user>/mlops-locust` |

## Variantes de la imagen de Airflow

| Imagen | Dockerfile | Uso |
|---|---|---|
| `mlops-airflow` | `Dockerfile` | Producción / Kubernetes — DAGs baked en la imagen, sin montaje de volúmenes |
| `mlops-airflow-compose` | `Dockerfile.Compose` | Desarrollo local — imagen ligera, DAGs se montan como volumen en Docker Compose |

Ambas variantes se construyen con `fail-fast: false`, de modo que un fallo en una no cancela la otra.

## Estrategia de etiquetas (tags)

| Situación | Tags generadas |
|---|---|
| Push a `main` | `sha-<7chars>` + `latest` |
| Push a rama `feature/**` | `sha-<7chars>` + `dev` |
| Pull Request hacia `main` | Solo build local, sin push |
| `workflow_dispatch` desde `main` | `sha-<7chars>` + `latest` |
| `workflow_dispatch` desde otra rama | `sha-<7chars>` + `dev` |

El tag `sha-<7chars>` garantiza trazabilidad exacta de cada imagen al commit que la generó. `latest` y `dev` son etiquetas móviles que apuntan siempre al build más reciente de su rama.

## Secretos requeridos

Configura los siguientes secretos en **Settings → Secrets and variables → Actions** del repositorio:

| Secreto | Descripción |
|---|---|
| `DOCKERHUB_USERNAME` | Usuario de DockerHub (ej. `miusuario`) |
| `DOCKERHUB_TOKEN` | Access token de DockerHub — genéralo en hub.docker.com → Account Settings → Security → New Access Token |

## Triggers por paths

Cada workflow solo se ejecuta cuando cambian los archivos relevantes de su componente:

| Workflow | Paths que lo disparan |
|---|---|
| `build-api.yml` | `Proyecto_3/api/**` |
| `build-streamlit.yml` | `Proyecto_3/streamlit/**` |
| `build-airflow.yml` | `Proyecto_3/Airflow/Dockerfile`, `Dockerfile.Compose`, `requirements.txt`, `dags/**` |
| `build-mlflow.yml` | `Proyecto_3/mlflow/**` |
| `build-observability.yml` | `Proyecto_3/prometheus/**`, `Proyecto_3/grafana/**`, `Proyecto_3/locust/**` |

Cualquier workflow también se dispara si su propio archivo `.yml` cambia.

## Ejecución manual

Todos los workflows tienen `workflow_dispatch`, lo que permite dispararlos desde la pestaña **Actions** de GitHub sin necesidad de hacer un commit.
