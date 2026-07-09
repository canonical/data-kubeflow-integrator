apiVersion: v1
kind: Secret
metadata:
  name: {{ secret.name }}
  {% if secret.namespace %}
  namespace: {{ secret.namespace }}
  {% endif %}
type: Opaque
data:
  {% for key, value in secret.data.items() %}
  {{ key }}: {{ value }}
  {% endfor %}
