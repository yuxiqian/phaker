# phaker

"Faker" pipeline connector for Flink CDC 3.x. This could be used for large-scale fuzzy testing sink pipeline connectors or runtime operators.

## usage

`sbt package` and copy `flink-cdc-pipeline-connector-phaker` to CDC `/lib` path, and it should be available like this:

```yaml
source:
  type: phaker
  table.id: default_namespace.default_schema # Fully qualified Table ID
  rejected.types: BinaryType,VarBinaryType   # Exclude some data types if downstream could not handle them
  schema.evolve: true                        # Generate schema evolution events, too
  max.column.count: 50                       # limit maximum column count
  records.per.second: 17                     # maximum records emitted each second
```

## example

```
CreateTableEvent{tableId=default_namespace.default_schema.table_name, schema=columns={`id` BIGINT}, primaryKeys=id, options=()}
DataChangeEvent{tableId=default_namespace.default_schema.table_name, before=[], after=[1], op=INSERT, meta=()}
...
AddColumnEvent{tableId=default_namespace.default_schema.table_name, addedColumns=[ColumnWithPosition{column=`column1` VARBINARY(76), position=LAST, existedColumnName=null}]}
DataChangeEvent{tableId=default_namespace.default_schema.table_name, before=[], after=[18, [B@222d0385], op=INSERT, meta=()}
DataChangeEvent{tableId=default_namespace.default_schema.table_name, before=[18, [B@75f69728], after=[18, [B@5e59cd8a], op=UPDATE, meta=()}
...
AddColumnEvent{tableId=default_namespace.default_schema.table_name, addedColumns=[ColumnWithPosition{column=`column2` CHAR(80), position=LAST, existedColumnName=null}]}
DataChangeEvent{tableId=default_namespace.default_schema.table_name, before=[], after=[35, [B@9f374e2, 蟮藝ᐮ㒥먬⬸], op=INSERT, meta=()}
DataChangeEvent{tableId=default_namespace.default_schema.table_name, before=[35, [B@32ae4f19, 蟮藝ᐮ㒥먬⬸], after=[], op=DELETE, meta=()}
...
```
