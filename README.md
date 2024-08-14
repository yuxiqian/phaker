# phaker

"Faker" pipeline connector for Flink CDC 3.x. This could be used for large-scale fuzzy testing sink pipeline connectors or runtime operators.

## usage

`sbt package` and copy `flink-cdc-pipeline-connector-phaker` to CDC `/lib` path, and it should be available like this:

```yaml
source:
  type: phaker
  namespace.name: default_namespace
  schema.name: default_schema
  table.name: table_name
  schema.evolve: true    # Generate schema evolution events, too
  max.column.count: 50   # limit maximum column count
  batch.count: 17        # how many data records should source emits in each batch
  sleep.time: 1000       # sleep duration (in ms) between two consecutive batches
```
