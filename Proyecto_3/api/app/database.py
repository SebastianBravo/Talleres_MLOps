import json
import uuid
from datetime import datetime, timezone

import psycopg2

from .config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
        connect_timeout=5,
    )


def ensure_inference_logs_table():
    """Creates inference_logs if it doesn't exist. Fails silently so startup is not blocked."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inference_logs (
                request_id       UUID      PRIMARY KEY DEFAULT gen_random_uuid(),
                requested_at     TIMESTAMP NOT NULL,
                input_data       JSONB     NOT NULL,
                predicted_price  FLOAT     NOT NULL,
                model_name       VARCHAR(128),
                model_version    VARCHAR(32),
                model_alias      VARCHAR(64),
                response_time_ms FLOAT     NOT NULL
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("Tabla inference_logs verificada.")
    except Exception as exc:
        print(f"No se pudo crear inference_logs: {exc}")


def log_inference(
    input_data: dict,
    predicted_price: float,
    response_time_ms: float,
    model_info: dict,
):
    """Persists one inference record. Fails silently to avoid disrupting predictions."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO inference_logs
                (request_id, requested_at, input_data, predicted_price,
                 model_name, model_version, model_alias, response_time_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                datetime.now(timezone.utc),
                json.dumps(input_data),
                float(predicted_price),
                model_info.get("name"),
                model_info.get("version"),
                model_info.get("alias"),
                response_time_ms,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        print(f"Error al registrar inferencia: {exc}")
