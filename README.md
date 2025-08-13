# golang-gcs-writer-test-01
simple Golang web service that writes a bunch of objects to a GCS bucket 

Usage:
```
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"number": 10}' \
  http://localhost:8080/
```