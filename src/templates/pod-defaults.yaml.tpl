apiVersion: kubeflow.org/v1alpha1
kind: PodDefault
metadata:
  name: {{ pod_default.name }}
  {% if pod_default.namespace %}
  namespace: {{ pod_default.namespace }}
  {% endif %}
spec:
  selector:
    matchLabels:
      {{ pod_default.selector_name }}: "true"
  desc: {{ pod_default.desc }}
  env:
  {% for env_var in pod_default.env_vars %}
  - name: {{ env_var.name }}
    {% if env_var.value %}
    value: {{ env_var.value }}
    {% endif %}
    {% if env_var.secret %}
    valueFrom:
      secretKeyRef:
        name: {{ env_var.secret.secret_name }}
        key: {{ env_var.secret.secret_key }}
        optional: {{ env_var.secret.optional | tojson }}
    {% endif %}
  {% endfor%}
