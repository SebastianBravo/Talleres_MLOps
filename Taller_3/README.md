# Taller 3 - Apache Airflow con Docker Compose

Este proyecto despliega un entorno completo de Apache Airflow utilizando Docker Compose, con servicios adicionales para orquestación de flujos de trabajo y almacenamiento de datos.

## Servicios Desplegados

El `docker-compose.yaml` despliega los siguientes servicios:

### Servicios de Airflow
- **airflow-webserver**: Interfaz web de Airflow (puerto 8080)
- **airflow-scheduler**: Programador de tareas de Airflow
- **airflow-worker**: Trabajador Celery para ejecutar tareas
- **airflow-triggerer**: Servicio para triggers asíncronos
- **airflow-init**: Servicio de inicialización (crea usuario admin y configura la base de datos)

### Servicios de Soporte
- **postgres**: Base de datos PostgreSQL para metadata de Airflow
- **redis**: Broker de mensajes para CeleryExecutor
- **mysql_db**: Base de datos MySQL para uso en los DAGs

### Configuración
- **Executor**: CeleryExecutor (permite ejecución distribuida de tareas)
- **Usuario por defecto**: airflow / airflow
- **Puerto web**: http://localhost:8080

## Estructura de Carpetas

```
Taller_3/
├── docker-compose.yaml        # Configuración de servicios Docker
├── .env                        # Variables de entorno (crear manualmente)
├── .env.example               # Ejemplo de variables de entorno
└── Airflow/
    ├── Dockerfile             # Imagen personalizada de Airflow
    ├── requirements.txt       # Dependencias Python para los DAGs
    ├── config/               # Archivos de configuración personalizados
    ├── dags/                 # Directorio de DAGs
    │   └── test.py          # DAG de ejemplo
    ├── logs/                # Logs de ejecución de Airflow
    └── plugins/             # Plugins personalizados de Airflow
```

## Dockerfile Personalizado

El proyecto utiliza un Dockerfile personalizado que extiende la imagen oficial de Apache Airflow (`apache/airflow:2.11.1`). Este Dockerfile instala automáticamente las dependencias necesarias para los DAGs desde el archivo `requirements.txt`, que incluye:

- `mysql-connector-python`: Para conectar con bases de datos MySQL
- `palmerpenguins`: Dataset de ejemplo para análisis
- `pandas`: Manipulación y análisis de datos
- `scikit-learn`: Biblioteca de machine learning

Esto permite que todos los contenedores de Airflow tengan las mismas dependencias instaladas sin necesidad de instalarlas en tiempo de ejecución.

## Pasos para Configurar y Ejecutar

### 1. Crear las Carpetas Necesarias

En la carpeta `Airflow`, crear los directorios para configuración, logs y plugins:

```bash
mkdir -p ./Airflow/config ./Airflow/logs ./Airflow/plugins
```

O si ya estás dentro de la carpeta Airflow:

```bash
mkdir -p ./config ./logs ./plugins
```

### 2. Configurar Variables de Entorno

Crear un archivo `.env` en la raíz del proyecto (Taller_3) con las siguientes variables:

#### En Linux/macOS:
```bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_PROJ_DIR=./Airflow" > .env
```

#### En Windows (Git Bash):
```bash
echo -e "AIRFLOW_UID=50000\nAIRFLOW_PROJ_DIR=./Airflow" > .env
```

#### Manualmente:
Crea el archivo `.env` con el siguiente contenido:
```
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./Airflow
```

> **Nota**: En Linux/macOS, `AIRFLOW_UID` debe coincidir con tu UID de usuario para evitar problemas de permisos. En Windows, puedes usar 50000 como valor por defecto.

### 3. Construir y Levantar los Servicios

Ejecutar el siguiente comando desde la carpeta `Taller_3`:

```bash
docker compose up --build
```

Este comando:
- Construye la imagen personalizada de Airflow con las dependencias
- Inicia todos los servicios definidos en docker-compose.yaml
- Inicializa la base de datos y crea el usuario administrador

### 4. Acceder a Airflow

Una vez que todos los servicios estén corriendo:

1. Abrir el navegador en: http://localhost:8080
2. Iniciar sesión con las credenciales:
   - **Usuario**: airflow
   - **Contraseña**: airflow

## Variables de Entorno MySQL

Los DAGs tienen acceso a las siguientes variables de entorno para conectarse a MySQL:

- `MYSQL_HOST`: mysql_db
- `MYSQL_USER`: airflow
- `MYSQL_PASSWORD`: airflow
- `MYSQL_DATABASE`: airflow

Estas variables están configuradas en el `docker-compose.yaml` y son accesibles desde cualquier tarea de los DAGs.

## Comandos Útiles

### Detener los servicios
```bash
docker compose down
```

### Ver logs de un servicio específico
```bash
docker compose logs airflow-webserver
docker compose logs airflow-scheduler
```

### Reconstruir las imágenes
```bash
docker compose build
```

### Limpiar volúmenes (¡cuidado! elimina datos)
```bash
docker compose down -v
```

## Ejemplo de DAG

El archivo `dags/test.py` contiene un DAG de ejemplo que:
1. Carga el dataset de Palmer Penguins
2. Se conecta a la base de datos MySQL

Este DAG demuestra cómo usar las dependencias instaladas y cómo conectarse a MySQL desde los DAGs.

## Troubleshooting

### Error de permisos
Si encuentras errores de permisos, verifica que `AIRFLOW_UID` en `.env` coincida con tu UID de usuario (en Linux/macOS).

### Los DAGs no aparecen
- Verifica que los archivos estén en la carpeta `Airflow/dags/`
- Revisa los logs del scheduler: `docker compose logs airflow-scheduler`
- Asegúrate de que no haya errores de sintaxis en el DAG

### Servicios no inician
- Verifica que tienes suficientes recursos (4GB RAM, 2 CPUs recomendados)
- Revisa los logs: `docker compose logs`
