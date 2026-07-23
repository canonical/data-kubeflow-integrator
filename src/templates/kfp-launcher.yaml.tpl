apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ name }}
  {% if namespace %}
  namespace: {{ namespace }}
  {% endif %}
data:
  defaultPipelineRoot: {{ default_pipeline_root | tojson }}
  providers: |
    minio:
      default:
        endpoint: {{ endpoint | tojson }}
        disableSSL: {{ disable_ssl | tojson }}
        region: {{ region | tojson }}
        credentials:
          fromEnv: false
          secretRef:
            secretName: {{ secret_name }}
            accessKeyKey: {{ access_key }}
            secretKeyKey: {{ secret_key }}
