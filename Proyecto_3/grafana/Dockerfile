FROM grafana/grafana:12.0.1

# Bake provisioning configs and dashboards into the image.
# /etc/grafana/provisioning is read by Grafana at startup.
# /etc/grafana/dashboards is the path declared in provisioning/dashboards/dashboard.yml
# and does not overlap with the runtime data volume at /var/lib/grafana.
COPY provisioning/datasources/ /etc/grafana/provisioning/datasources/
COPY provisioning/dashboards/  /etc/grafana/provisioning/dashboards/
COPY dashboards/               /etc/grafana/dashboards/
