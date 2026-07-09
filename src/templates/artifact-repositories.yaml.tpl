apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ name }}
  {% if namespace %}
  namespace: {{ namespace }}
  {% endif %}
  annotations:
    {{ annotation_key }}: {{ annotation_ref }}
data:
  {{ annotation_ref }}: |
    archiveLogs: true
    s3:
      accessKeySecret:
        name: {{ secret_name }}
        key: {{ access_key }}
      secretKeySecret:
        name: {{ secret_name }}
        key: {{ secret_key }}
      bucket: {{ bucket | tojson }}
      endpoint: {{ endpoint | tojson }}
      insecure: {{ insecure | tojson }}
      keyFormat: {{ key_format | tojson }}
