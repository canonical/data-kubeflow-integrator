apiVersion: v1
kind: Secret
metadata:
  {% if secret.namespace %}
  namespace: {{ secret.namespace }}
  {% endif %}
  name: {{ secret.name }}
  labels:
    user.kubeflow.org/enabled: "true"
    {% if secret.labels %}
	{% for key, value in secret.labels.items() %}
	{{ key }}: {{ value }}
	{% endfor %}
    {% endif %}
type: Opaque 
data:
  {% for key,value in secret.data.items() %}
  {{ key }}: {{ value }}
  {% endfor %}
