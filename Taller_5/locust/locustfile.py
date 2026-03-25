from locust import HttpUser, task, between

class UsuarioDeCarga(HttpUser):
    wait_time = between(1, 2.5)

    @task
    def hacer_inferencia(self):
        payload = {
            "bill_length_mm": 0,
            "bill_depth_mm": 0,
            "flipper_length_mm": 0,
            "body_mass_g": 0,
            "year": 2009,
            "island_Biscoe": 0,
            "island_Dream": 0,
            "island_Torgersen": 0,
            "sex_female": 0,
            "sex_male": 0
        }
        # Enviar una petición POST al endpoint /predict
        response = self.client.post("/predict", json=payload)
        # Opcional: validación de respuesta
        if response.status_code != 200:
            print("❌ Error en la inferencia:", response.text)