from locust import HttpUser, task, between

# Payload de ejemplo basado en un paciente real del dataset de diabetes
EXAMPLE_PAYLOAD = {
    "race": "Caucasian",
    "gender": "Female",
    "age": "[70-80)",
    "weight": None,
    "admission_type_id": 1,
    "discharge_disposition_id": 3,
    "admission_source_id": 7,
    "time_in_hospital": 5,
    "payer_code": "MC",
    "medical_specialty": "InternalMedicine",
    "num_lab_procedures": 45,
    "num_procedures": 1,
    "num_medications": 18,
    "number_outpatient": 0,
    "number_emergency": 0,
    "number_inpatient": 1,
    "diag_1": "428",
    "diag_2": "250",
    "diag_3": "401",
    "number_diagnoses": 9,
    "max_glu_serum": "None",
    "a1cresult": "None",
    "metformin": "No",
    "repaglinide": "No",
    "nateglinide": "No",
    "chlorpropamide": "No",
    "glimepiride": "No",
    "acetohexamide": "No",
    "glipizide": "No",
    "glyburide": "No",
    "tolbutamide": "No",
    "pioglitazone": "No",
    "rosiglitazone": "No",
    "acarbose": "No",
    "miglitol": "No",
    "troglitazone": "No",
    "tolazamide": "No",
    "examide": "No",
    "citoglipton": "No",
    "insulin": "Steady",
    "glyburide_metformin": "No",
    "glipizide_metformin": "No",
    "glimepiride_pioglitazone": "No",
    "metformin_rosiglitazone": "No",
    "metformin_pioglitazone": "No",
    "change": "Ch",
    "diabetesmed": "Yes",
}


class DiabetesAPIUser(HttpUser):
    # Tiempo de espera entre tareas: simula usuario real (1 a 3 segundos)
    wait_time = between(1, 3)

    @task(5)
    def predict(self):
        """Tarea principal: solicitud de predicción al endpoint /predict."""
        with self.client.post(
            "/predict",
            json=EXAMPLE_PAYLOAD,
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 503:
                # Modelo no cargado aún — no es un fallo del servidor
                response.failure("Modelo no disponible (503)")
            else:
                response.failure(f"Error inesperado: {response.status_code}")

    @task(1)
    def health_check(self):
        """Tarea secundaria: verifica que la API esté activa."""
        with self.client.get("/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Health check falló: {response.status_code}")
