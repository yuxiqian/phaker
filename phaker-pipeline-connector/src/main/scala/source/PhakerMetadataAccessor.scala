package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.event.TableId
import org.apache.flink.cdc.common.schema.Schema
import org.apache.flink.cdc.common.source.MetadataAccessor

import java.util
import scala.collection.JavaConverters._

class PhakerMetadataAccessor(
    namespaceName: String,
    schemaName: String,
    tableName: String
) extends MetadataAccessor {

  def apply(
      namespaceName: String,
      schemaName: String,
      tableName: String
  ): PhakerMetadataAccessor =
    new PhakerMetadataAccessor(namespaceName, schemaName, tableName)

  override def listNamespaces(): util.List[String] = List(namespaceName).asJava

  override def listSchemas(namespace: String): util.List[String] = {
    if (namespace == namespaceName) List(schemaName) else List.empty[String]
  }.asJava

  override def listTables(
      namespace: String,
      schema: String
  ): util.List[TableId] = {
    if (namespace == namespaceName && schema == schemaName)
      List(TableId.tableId(namespaceName, schemaName, tableName))
    else List.empty[TableId]
  }.asJava

  override def getTableSchema(tableId: TableId): Schema = {
    PhakerDatabase.genSchema
  }
}
