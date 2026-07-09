apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ name }}
  {% if namespace %}
  namespace: {{ namespace }}
  {% endif %}
data:
  defaultPipelineRoot: "{{ default_pipeline_root }}"
  providers: |
    s3:
      default:
        endpoint: "{{ endpoint }}"
        disableSSL: {{ disable_ssl | lower }}
        region: "{{ region }}"
        credentials:
          fromEnv: false
          secretRef:
            secretName: {{ secret_name }}
            accessKeyKey: {{ access_key }}
            secretKeyKey: {{ secret_key }}
