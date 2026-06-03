REGISTRY  = bravosjs
TAG       = dev
MANIFESTS = manifests

NS_INFRA   = mlops-infra
NS_APP     = mlops-app
NS_OBS     = mlops-obs
NS_AIRFLOW = mlops-airflow

# ─────────────────────────────────────────────
# KUBERNETES — namespaces
# ─────────────────────────────────────────────

namespaces:
	kubectl apply -f $(MANIFESTS)/namespaces/

# ─────────────────────────────────────────────
# KUBERNETES — despliegue por capas
# ─────────────────────────────────────────────

deploy-infra: namespaces
	kubectl apply -f $(MANIFESTS)/infra/postgres-dataset/ -n $(NS_INFRA)
	kubectl apply -f $(MANIFESTS)/infra/minio/            -n $(NS_INFRA)
	kubectl apply -f $(MANIFESTS)/infra/mlflow/           -n $(NS_INFRA)
	kubectl apply -f $(MANIFESTS)/infra/api-source/       -n $(NS_INFRA)

wait-infra:
	kubectl wait --for=condition=ready pod -l app=postgres-dataset -n $(NS_INFRA) --timeout=300s
	kubectl wait --for=condition=ready pod -l app=minio            -n $(NS_INFRA) --timeout=300s
	kubectl wait --for=condition=ready pod -l app=mlflow-db        -n $(NS_INFRA) --timeout=300s
	kubectl wait --for=condition=ready pod -l app=mlflow           -n $(NS_INFRA) --timeout=300s
	kubectl wait --for=condition=ready pod -l app=api-source       -n $(NS_INFRA) --timeout=120s

minio-init:
	kubectl delete pod minio-init -n $(NS_INFRA) --ignore-not-found
	kubectl apply  -f $(MANIFESTS)/infra/minio-init-pod.yaml -n $(NS_INFRA)
	kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/minio-init -n $(NS_INFRA) --timeout=120s

deploy-app:
	kubectl apply -f $(MANIFESTS)/app/api/       -n $(NS_APP)
	kubectl apply -f $(MANIFESTS)/app/streamlit/ -n $(NS_APP)

deploy-obs:
	kubectl apply -f $(MANIFESTS)/obs/prometheus/ -n $(NS_OBS)
	kubectl apply -f $(MANIFESTS)/obs/grafana/    -n $(NS_OBS)
	kubectl apply -f $(MANIFESTS)/obs/locust/     -n $(NS_OBS)

airflow-migrate:
	kubectl delete pod airflow-migrate -n $(NS_AIRFLOW) --ignore-not-found
	@echo "Esperando secret airflow-metadata..."
	@until kubectl get secret airflow-metadata -n $(NS_AIRFLOW) > /dev/null 2>&1; do \
		echo "  ..."; sleep 3; \
	done
	kubectl run airflow-migrate \
		--image=bravosjs/mlops-airflow:dev \
		--restart=Never \
		--namespace=$(NS_AIRFLOW) \
		--env="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$$(kubectl get secret airflow-metadata -n $(NS_AIRFLOW) -o jsonpath='{.data.connection}' | base64 -d)" \
		-- airflow db migrate
	kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
		pod/airflow-migrate -n $(NS_AIRFLOW) --timeout=180s
	@echo "Migraciones de Airflow aplicadas."

# Instala/actualiza el chart de Helm de Airflow y espera a su PostgreSQL.
airflow-helm:
	@which helm > /dev/null 2>&1 || { echo "ERROR: 'helm' no esta instalado o no esta en el PATH."; echo "Instalalo (Mac: brew install helm | Windows: winget install Helm.Helm)"; exit 1; }
	helm repo add apache-airflow https://airflow.apache.org --force-update
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(NS_AIRFLOW) \
		--create-namespace \
		-f $(MANIFESTS)/airflow-helm-values/values-local.yaml \
		--timeout 5m
	@echo "Esperando PostgreSQL de Airflow..."
	kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=postgresql \
		-n $(NS_AIRFLOW) --timeout=120s

# Despliega Airflow: instala el chart y luego aplica las migraciones.
# Se usan prerequisitos (no $(MAKE) recursivo) para que funcione igual en
# Mac/Linux y en Windows, donde la ruta de make tiene espacios y parentesis
# que rompen la llamada recursiva via shell.
deploy-airflow: airflow-helm airflow-migrate
	@echo "Airflow desplegado en el namespace $(NS_AIRFLOW)."

# Herramientas opcionales (no forman parte del pipeline principal)
deploy-adminer:
	kubectl apply -f $(MANIFESTS)/infra/adminer/ -n $(NS_INFRA)

# Despliegue imperativo (sin Argo) — se conserva como respaldo.
deploy-kubectl: namespaces deploy-infra wait-infra minio-init deploy-app deploy-obs deploy-airflow

# ─────────────────────────────────────────────
# GITOPS — Argo CD (flujo principal de despliegue)
# ─────────────────────────────────────────────

ARGOCD_NS = argocd

# Instala Argo CD en el clúster (paso de bootstrap, una sola vez).
# Se usa --server-side porque el CRD applicationsets.argoproj.io es demasiado
# grande para el apply client-side (la anotacion last-applied supera 256 KB).
argocd-install:
	kubectl create namespace $(ARGOCD_NS) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply --server-side --force-conflicts -n $(ARGOCD_NS) -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
	@echo "Esperando a que Argo CD este listo..."
	kubectl wait --for=condition=available deployment/argocd-server -n $(ARGOCD_NS) --timeout=300s
	@echo "Argo CD instalado."

# Aplica el AppProject y la Application raiz (App-of-Apps).
# A partir de aqui Argo sincroniza namespaces -> infra -> app -> obs desde Git.
argocd-bootstrap:
	kubectl apply -f $(MANIFESTS)/argocd/project.yaml
	kubectl apply -f $(MANIFESTS)/argocd/root-app.yaml
	@echo "Application raiz aplicada. Argo CD comenzara a sincronizar las capas."
	@echo "Airflow (wave 3) sera desplegado automaticamente via Helm por Argo CD."

# Muestra la contrasena inicial del usuario admin de la UI de Argo CD.
argocd-password:
	@kubectl -n $(ARGOCD_NS) get secret argocd-initial-admin-secret \
		-o jsonpath='{.data.password}' | base64 -d; echo

# Abre la UI de Argo CD en https://localhost:8083
argocd-forward:
	kubectl port-forward svc/argocd-server 8083:443 -n $(ARGOCD_NS) &
	@echo "  Argo CD UI       -> https://localhost:8083  (admin / make argocd-password)"

# Estado de las Applications gestionadas por Argo CD.
argocd-status:
	kubectl get applications -n $(ARGOCD_NS)

# Espera a que Argo CD cree y deje sanas las Applications de cada capa.
# Las hijas se crean por olas (sync-waves): namespaces -> infra -> app/obs -> airflow.
# Por eso se espera primero a que CADA Application exista y luego a que quede
# Healthy, en orden de ola.
argocd-wait:
	@echo "Esperando a que Argo CD cree y sincronice las Applications (waves 0->3)..."
	@for app in mlops-namespaces mlops-infra mlops-app mlops-obs mlops-airflow; do \
		echo "  -> esperando a que exista $$app..."; \
		until kubectl get application $$app -n $(ARGOCD_NS) >/dev/null 2>&1; do sleep 5; done; \
		echo "  -> esperando a que $$app quede Healthy..."; \
		kubectl wait --for=jsonpath='{.status.health.status}'=Healthy application/$$app -n $(ARGOCD_NS) --timeout=600s; \
	done
	@echo "Todas las capas (namespaces/infra/app/obs/airflow) sincronizadas por Argo CD."

# Despliegue completo del proyecto VIA ARGO CD (flujo principal).
# Instala Argo, aplica el App-of-Apps y espera a que todas las capas sincronicen.
# Airflow se despliega como wave 3 gestionado por el chart de Helm de Argo CD.
deploy: argocd-install argocd-bootstrap argocd-wait
	@echo ""
	@echo "Proyecto desplegado. Argo CD gestiona todas las capas incluyendo Airflow."
	@echo "UI de Argo:  make argocd-forward   ->  https://localhost:8083"
	@echo "Servicios:   make forward"

# ─────────────────────────────────────────────
# KUBERNETES — acceso local (port-forward)
# ─────────────────────────────────────────────

forward:
	kubectl port-forward svc/mlops-airflow-webserver 8080:8080 -n $(NS_AIRFLOW) &
	kubectl port-forward svc/mlflow            5000:5000 -n $(NS_INFRA)   &
	kubectl port-forward svc/api               8000:8000 -n $(NS_APP)     &
	kubectl port-forward svc/streamlit         8501:8501 -n $(NS_APP)     &
	kubectl port-forward svc/locust            8089:8089 -n $(NS_OBS)     &
	kubectl port-forward svc/prometheus        9090:9090 -n $(NS_OBS)     &
	kubectl port-forward svc/grafana           3000:3000 -n $(NS_OBS)     &
	kubectl port-forward svc/postgres-dataset  5432:5432 -n $(NS_INFRA)   &
	kubectl port-forward svc/mlflow-db         5433:5432 -n $(NS_INFRA)   &
	kubectl port-forward svc/minio             9001:9001 -n $(NS_INFRA)   &
	kubectl port-forward svc/api-source        8001:80   -n $(NS_INFRA)   &
	kubectl port-forward svc/argocd-server     8083:443  -n $(ARGOCD_NS)  &
	@echo ""
	@echo "Servicios disponibles:"
	@echo "  Argo CD          → https://localhost:8083 (admin / make argocd-password)"
	@echo "  Airflow          → http://localhost:8080  (airflow / airflow)"
	@echo "  MLflow           → http://localhost:5000"
	@echo "  API              → http://localhost:8000/docs"
	@echo "  Streamlit        → http://localhost:8501"
	@echo "  Locust           → http://localhost:8089"
	@echo "  Prometheus       → http://localhost:9090"
	@echo "  Grafana          → http://localhost:3000  (admin / admin)"
	@echo "  DB dataset       → localhost:5432  (airflow / airflow → real_estate_data)"
	@echo "  DB MLflow        → localhost:5433  (mlflow / mlflow   → mlflow)"
	@echo "  MinIO UI         → http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Source API       → http://localhost:8001"

forward-adminer:
	kubectl port-forward svc/adminer 8082:8080 -n $(NS_INFRA) &
	@echo "  Adminer          → http://localhost:8082"

stop-forward:
	pkill -f "kubectl port-forward" || true

# ─────────────────────────────────────────────
# KUBERNETES — estado y diagnóstico
# ─────────────────────────────────────────────

status:
	@echo "\n── $(NS_INFRA) ──────────────────────────────────"
	kubectl get pods,svc -n $(NS_INFRA)
	@echo "\n── $(NS_APP) ────────────────────────────────────"
	kubectl get pods,svc -n $(NS_APP)
	@echo "\n── $(NS_OBS) ───────────────────────────────────"
	kubectl get pods,svc -n $(NS_OBS)
	@echo "\n── $(NS_AIRFLOW) ────────────────────────────────"
	kubectl get pods,svc -n $(NS_AIRFLOW)

restart-api:
	kubectl rollout restart deployment/api -n $(NS_APP)
	kubectl rollout status  deployment/api -n $(NS_APP)

restart-airflow:
	kubectl rollout restart deployment/airflow-scheduler  -n $(NS_AIRFLOW)
	kubectl rollout restart deployment/airflow-webserver  -n $(NS_AIRFLOW)

# ─────────────────────────────────────────────
# GESTIÓN DE RECURSOS — modo pipeline / modo completo
# ─────────────────────────────────────────────

# Libera RAM apagando streamlit, observabilidad y Argo CD mientras el DAG corre.
# La API de inferencia (mlops-app/api) se mantiene activa porque el DAG la usa.
# Orden: 1) deshabilitar selfHeal (Argo aún activo), 2) bajar servicios, 3) bajar Argo.
pipeline-mode:
	@echo "1/3 Desactivando selfHeal en Argo CD (app y obs)..."
	kubectl patch application mlops-app -n $(ARGOCD_NS) --type merge \
		-p '{"spec":{"syncPolicy":{"automated":{"selfHeal":false}}}}'
	kubectl patch application mlops-obs -n $(ARGOCD_NS) --type merge \
		-p '{"spec":{"syncPolicy":{"automated":{"selfHeal":false}}}}'
	@echo "2/3 Escalando a 0 streamlit y observabilidad..."
	kubectl scale deployment streamlit  --replicas=0 -n $(NS_APP)
	kubectl scale deployment prometheus --replicas=0 -n $(NS_OBS)
	kubectl scale deployment grafana    --replicas=0 -n $(NS_OBS)
	kubectl scale deployment locust     --replicas=0 -n $(NS_OBS)
	@echo "3/3 Escalando a 0 los componentes de Argo CD..."
	kubectl scale deployment argocd-server                  --replicas=0 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-repo-server             --replicas=0 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-dex-server              --replicas=0 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-redis                   --replicas=0 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-notifications-controller --replicas=0 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-applicationset-controller --replicas=0 -n $(ARGOCD_NS)
	kubectl scale statefulset argocd-application-controller --replicas=0 -n $(ARGOCD_NS)
	@echo ""
	@echo "Modo pipeline activo. Corren: infra + api + airflow."
	@echo "Cuando el DAG termine, ejecuta: make full-mode"

# Reactiva Argo CD, luego restaura selfHeal y sube streamlit y observabilidad.
# Orden inverso: 1) subir Argo, 2) restaurar selfHeal, 3) subir servicios.
full-mode:
	@echo "1/3 Escalando a 1 los componentes de Argo CD..."
	kubectl scale statefulset argocd-application-controller  --replicas=1 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-redis                    --replicas=1 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-repo-server              --replicas=1 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-dex-server               --replicas=1 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-server                   --replicas=1 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-notifications-controller  --replicas=1 -n $(ARGOCD_NS)
	kubectl scale deployment argocd-applicationset-controller --replicas=1 -n $(ARGOCD_NS)
	@echo "Esperando a que Argo CD este listo..."
	kubectl wait --for=condition=available deployment/argocd-server -n $(ARGOCD_NS) --timeout=120s
	@echo "2/3 Reactivando selfHeal en Argo CD (app y obs)..."
	kubectl patch application mlops-app -n $(ARGOCD_NS) --type merge \
		-p '{"spec":{"syncPolicy":{"automated":{"selfHeal":true}}}}'
	kubectl patch application mlops-obs -n $(ARGOCD_NS) --type merge \
		-p '{"spec":{"syncPolicy":{"automated":{"selfHeal":true}}}}'
	@echo "3/3 Escalando a 1 streamlit y observabilidad..."
	kubectl scale deployment streamlit  --replicas=1 -n $(NS_APP)
	kubectl scale deployment prometheus --replicas=1 -n $(NS_OBS)
	kubectl scale deployment grafana    --replicas=1 -n $(NS_OBS)
	kubectl scale deployment locust     --replicas=1 -n $(NS_OBS)
	@echo ""
	@echo "Modo completo activo. Todos los servicios disponibles."
	@echo "Levanta los port-forwards con: make forward"

# ─────────────────────────────────────────────
# KUBERNETES — limpieza
# ─────────────────────────────────────────────

delete-app:
	kubectl delete namespace $(NS_APP)     --ignore-not-found
	kubectl delete namespace $(NS_OBS)     --ignore-not-found
	kubectl delete namespace $(NS_AIRFLOW) --ignore-not-found

delete:
	@echo "Eliminando finalizers de Applications (evita Terminating colgado)..."
	kubectl get applications -n $(ARGOCD_NS) -o name 2>/dev/null | \
		xargs -I{} kubectl patch {} -n $(ARGOCD_NS) --type merge \
		-p '{"metadata":{"finalizers":[]}}' || true
	kubectl delete namespace $(ARGOCD_NS) --ignore-not-found
	@kubectl get namespace $(ARGOCD_NS) -o json 2>/dev/null \
		| jq '.spec.finalizers = []' \
		| kubectl replace --raw /api/v1/namespaces/$(ARGOCD_NS)/finalize -f - 2>/dev/null || true
	kubectl delete namespace $(NS_INFRA)   --ignore-not-found
	kubectl delete namespace $(NS_APP)     --ignore-not-found
	kubectl delete namespace $(NS_OBS)     --ignore-not-found
	kubectl delete namespace $(NS_AIRFLOW) --ignore-not-found

.PHONY: namespaces \
        deploy-infra wait-infra minio-init deploy-app deploy-obs deploy-airflow airflow-helm airflow-migrate deploy deploy-kubectl \
        argocd-install argocd-bootstrap argocd-wait argocd-password argocd-forward argocd-status \
        deploy-adminer \
        pipeline-mode full-mode \
        forward forward-adminer stop-forward \
        status restart-api restart-airflow \
        delete-app delete
