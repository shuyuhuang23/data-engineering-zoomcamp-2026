# data-engineering-zoomcamp-2026
Data engineering zoomcamp hosted by DataTalks.Club

To create GCP infra

1. Set environment variable to point to your downloaded GCP keys

```
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Refresh token/session, and verify authentication
gcloud auth application-default login
```

2. Execute terraform to create GCP infra

```
terraform init
terraform apply
```

