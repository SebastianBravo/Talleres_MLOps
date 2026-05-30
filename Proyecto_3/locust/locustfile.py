from locust import HttpUser, between, task

PREDICT_PAYLOAD = {
    "bed": 3,
    "bath": 2.0,
    "acre_lot": 0.12,
    "house_size": 1850.0,
    "prev_sold_date": "2021-08-15",
    "brokered_by": "Realty Group Inc",
    "street": "123 Maple St",
    "city": "Austin",
    "state": "Texas",
    "zip_code": "78701",
    "status": "for_sale",
}


class RealEstateAPIUser(HttpUser):
    wait_time = between(1, 3)

    @task
    def predict(self):
        with self.client.post("/predict", json=PREDICT_PAYLOAD, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 503:
                response.failure("Modelo no disponible (503)")
            else:
                response.failure(f"Error inesperado: {response.status_code}")
