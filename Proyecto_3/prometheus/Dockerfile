FROM prom/prometheus:v3.4.0

# Bake the scrape configuration into the image so no volume mount is needed.
# For Kubernetes: update prometheus.yml and rebuild the image to change targets.
COPY prometheus.yml /etc/prometheus/prometheus.yml
