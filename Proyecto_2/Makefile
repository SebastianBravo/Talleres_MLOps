DOCKER_USER   ?= oscarariasback
NAMESPACE     ?= proyecto-2-mlops
TAG           ?= latest

# Imágenes propias del proyecto
IMG_API       = $(DOCKER_USER)/diabetes-api:$(TAG)
IMG_MLFLOW    = $(DOCKER_USER)/mlflow-local:$(TAG)
IMG_STREAMLIT = $(DOCKER_USER)/diabetes-streamlit:$(TAG)
IMG_LOCUST    = $(DOCKER_USER)/diabetes-locust:$(TAG)
IMG_AIRFLOW   = $(DOCKER_USER)/airflow-dags:$(TAG)

# ─────────────────────────────────────────────
# DOCKER COMPOSE
# ─────────────────────────────────────────────

compose-up:
	docker compose up -d --build

compose-down:
	docker compose down

compose-logs:
	docker compose logs -f

# ─────────────────────────────────────────────
# IMÁGENES — build y push
# ─────────────────────────────────────────────

build:
	docker build -t $(IMG_API)       ./api
	docker build -t $(IMG_MLFLOW)    ./mlflow
	docker build -t $(IMG_STREAMLIT) ./streamlit
	docker build -t $(IMG_LOCUST)    ./locust
	docker build -t $(IMG_AIRFLOW)   -f Airflow/Dockerfile ./Airflow

push:
	docker push $(IMG_API)
	docker push $(IMG_MLFLOW)
	docker push $(IMG_STREAMLIT)
	docker push $(IMG_LOCUST)
	docker push $(IMG_AIRFLOW)

build-push: build push

# ─────────────────────────────────────────────
# KUBERNETES — preparación
# ─────────────────────────────────────────────

# Actualiza el usuario de DockerHub en los manifiestos y values de Airflow
set-user:
	find komposefiles/ -name "*.yaml" | xargs sed -i '' "s|oscarariasblanco|$(DOCKER_USER)|g"
	sed -i '' "s|repository: airflow-local-dags|repository: $(DOCKER_USER)/airflow-dags|g" Airflow/values/values-local.yaml
	sed -i '' "s|tag: 0.0.1|tag: $(TAG)|g" Airflow/values/values-local.yaml

namespace:
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -

# ─────────────────────────────────────────────
# KUBERNETES — despliegue por capas
# ─────────────────────────────────────────────

deploy-infra:
	kubectl apply -f komposefiles/postgres_dataset/ -n $(NAMESPACE)
	kubectl apply -f komposefiles/minio/minio-data-persistentvolumeclaim.yaml -n $(NAMESPACE)
	kubectl apply -f komposefiles/minio/minio-deployment.yaml                 -n $(NAMESPACE)
	kubectl apply -f komposefiles/minio/minio-secret.yaml                     -n $(NAMESPACE)
	kubectl apply -f komposefiles/minio/minio-service.yaml                    -n $(NAMESPACE)
	kubectl apply -f komposefiles/mlflow/           -n $(NAMESPACE)

wait-infra:
	kubectl wait --for=condition=ready pod -l app=postgres-dataset -n $(NAMESPACE) --timeout=300s
	kubectl wait --for=condition=ready pod -l app=minio            -n $(NAMESPACE) --timeout=300s
	kubectl wait --for=condition=ready pod -l app=mlflow-db        -n $(NAMESPACE) --timeout=300s

minio-init:
	kubectl delete pod minio-init -n $(NAMESPACE) --ignore-not-found
	kubectl apply -f komposefiles/minio/minio-init-pod.yaml -n $(NAMESPACE)
	kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/minio-init -n $(NAMESPACE) --timeout=120s

deploy-api:
	kubectl apply -f komposefiles/api/ -n $(NAMESPACE)

deploy-ui:
	kubectl apply -f komposefiles/streamlit/ -n $(NAMESPACE)
	kubectl apply -f komposefiles/locust/    -n $(NAMESPACE)

deploy-obs:
	kubectl apply -f komposefiles/prometheus/ -n $(NAMESPACE)
	kubectl apply -f komposefiles/grafana/    -n $(NAMESPACE)

deploy-airflow:
	@which helm > /dev/null 2>&1 || brew install helm
	helm repo add apache-airflow https://airflow.apache.org
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(NAMESPACE) \
		-f Airflow/values/values-local.yaml

# Despliega todo en orden
deploy: namespace deploy-infra wait-infra minio-init deploy-api deploy-ui deploy-obs deploy-airflow

# ─────────────────────────────────────────────
# KUBERNETES — estado y logs
# ─────────────────────────────────────────────

status:
	kubectl get pods -n $(NAMESPACE)

logs:
	kubectl logs -l app=$(SVC) -n $(NAMESPACE) --tail=100 -f

describe:
	kubectl describe pod -l app=$(SVC) -n $(NAMESPACE)

# ─────────────────────────────────────────────
# KUBERNETES — acceso local (port-forward)
# ─────────────────────────────────────────────

forward:
	kubectl port-forward svc/airflow-webserver 8080:8080 -n $(NAMESPACE) &
	kubectl port-forward svc/mlflow            5000:5000 -n $(NAMESPACE) &
	kubectl port-forward svc/api               8000:8000 -n $(NAMESPACE) &
	kubectl port-forward svc/streamlit         8501:8501 -n $(NAMESPACE) &
	kubectl port-forward svc/locust            8089:8089 -n $(NAMESPACE) &
	kubectl port-forward svc/prometheus        9090:9090 -n $(NAMESPACE) &
	kubectl port-forward svc/grafana           3000:3000 -n $(NAMESPACE) &
	kubectl port-forward svc/postgres-dataset  5432:5432 -n $(NAMESPACE) &
	kubectl port-forward svc/mlflow-db         5433:5432 -n $(NAMESPACE) &
	kubectl port-forward svc/minio             9001:9001 -n $(NAMESPACE) &
	@echo ""
	@echo "Servicios disponibles:"
	@echo "  Airflow          → http://localhost:8080  (airflow / airflow)"
	@echo "  MLflow           → http://localhost:5000"
	@echo "  API              → http://localhost:8000/docs"
	@echo "  Streamlit        → http://localhost:8501"
	@echo "  Locust           → http://localhost:8089"
	@echo "  Prometheus       → http://localhost:9090"
	@echo "  Grafana          → http://localhost:3000  (admin / admin)"
	@echo "  DB dataset       → localhost:5432  (airflow / airflow  →  diabetic_data)"
	@echo "  DB MLflow        → localhost:5433  (mlflow / mlflow    →  mlflow)"
	@echo "  MinIO UI         → http://localhost:9001  (minioadmin / minioadmin)"

stop-forward:
	pkill -f "kubectl port-forward" || true

# ─────────────────────────────────────────────
# KUBERNETES — limpieza
# ─────────────────────────────────────────────

delete:
	kubectl delete namespace $(NAMESPACE)

.PHONY: compose-up compose-down compose-logs \
        build push build-push set-user namespace \
        deploy-infra wait-infra minio-init deploy-api deploy-ui deploy-obs deploy-airflow deploy \
        status logs describe forward stop-forward delete
