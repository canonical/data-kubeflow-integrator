apiVersion: v1
kind: Secret
metadata:
  namespace: {{ secret.namespace }}
  name: {{ secret.name }}
  labels:
    user.kubeflow.org/enabled: "true"
    {% if secret.labels %}
	{% for key, value in secret.labels.items() %}
	{{ key }}: {{ value }}
	{% endfor %}
    {% endif %}

stringData:
  {% for key,value in secret.data.items() %}
  {{ key }}: {{ value }}
  {% endfor %}
