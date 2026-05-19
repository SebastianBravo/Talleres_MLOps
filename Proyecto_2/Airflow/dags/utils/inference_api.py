"""Utilidades para interactuar con la API de inferencia."""

import os

import requests


def reload_inference_api(
    api_base_url=None,
    timeout_seconds=30,
):
    """
    Solicita a la API de inferencia recargar el modelo productivo desde MLflow.

    Esta función llama el endpoint POST /reload de la API. La API debe encargarse
    de consultar nuevamente MLflow y cargar el modelo que tenga el alias production.

    Parameters
    ----------
    api_base_url : str, optional
        URL base de la API de inferencia. Si no se envía, se toma desde la variable
        de entorno INFERENCE_API_URL.
    timeout_seconds : int
        Tiempo máximo de espera para la solicitud HTTP.

    Returns
    -------
    dict
        Respuesta JSON de la API.
    """

    base_url = api_base_url or os.environ.get(
        "INFERENCE_API_URL",
        "http://api:8000",
    )

    base_url = base_url.rstrip("/")
    reload_url = f"{base_url}/reload"

    print(f"Solicitando recarga del modelo en la API: {reload_url}")

    try:
        response = requests.post(reload_url, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(
            f"No fue posible recargar el modelo en la API de inferencia. "
            f"URL: {reload_url}. Error: {exc}"
        ) from exc

    try:
        response_json = response.json()
    except ValueError:
        response_json = {
            "status_code": response.status_code,
            "text": response.text,
        }

    print(f"Respuesta de la API de inferencia: {response_json}")

    return response_json