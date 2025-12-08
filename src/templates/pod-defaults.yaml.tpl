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
  {% if pod_default.secret_volumes %}
  volumes:
    {% for volume in pod_default.secret_volumes%}
    - name: {{ volume.name }}
      secret:
        secretName: {{volume.secret_name}}
    {% endfor %}
  volumeMounts:
    {% for volume in pod_default.secret_volumes%}
    - name: {{ volume.name }}
      mountPath: {{ volume.mount_path}}
    {% endfor %}
  {% endif %}
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
    {% if env_var.fieldref %}
    valueFrom:
      fieldRef:
        fieldPath: {{ env_var.fieldref.field_path }}
    {% endif %}
  {% endfor%}
  {% if pod_default.annotations %}
  annotations:
      {% for annotation in pod_default.annotations %}
      {{ annotation.key }}: {{ annotation.value }}
      {% endfor %}
  {% endif %}
  {% if pod_default.args %}
  args:
    {% for arg in pod_default.args %}
    - {{ arg }}
    {% endfor %}
  {% endif %}
  