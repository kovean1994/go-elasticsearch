# Example: Bulk Indexing from Kafka

-----

### NOTES

```
curl -X POST -H 'Content-Type: application/json' -H 'kbn-xsrf: true' 'http://localhost:5601/api/saved_objects/_export' -d '
{
  "objects": [
    {"type": "dashboard", "id": "48afb330-5a4c-11ea-a761-ab2c961503de"},
    {"type": "index-pattern", "id": "ecff41e0-5a4b-11ea-a761-ab2c961503de"}
  ],
  "includeReferencesDeep": true
}' > export.ndjson

curl -X POST -H 'kbn-xsrf: true' 'http://localhost:5601/api/saved_objects/_import?overwrite=true' --form file=@export.ndjson

open http://localhost:5601/app/kibana#/dashboard/48afb330-5a4c-11ea-a761-ab2c961503de
```
