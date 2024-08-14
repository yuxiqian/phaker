package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.event.TableId
import org.apache.flink.cdc.common.schema.Schema
import org.apache.flink.cdc.common.source.MetadataAccessor

import java.util
import scala.collection.JavaConverters._

class PhakerMetadataAccessor(
    tableId: TableId
) extends MetadataAccessor {

  override def listNamespaces(): util.List[String] = List(
    tableId.getNamespace
  ).asJava

  override def listSchemas(namespace: String): util.List[String] = {
    if (namespace == tableId.getNamespace) List(tableId.getSchemaName)
    else List.empty[String]
  }.asJava

  override def listTables(
      namespace: String,
      schema: String
  ): util.List[TableId] = {
    if (namespace == tableId.getNamespace && schema == tableId.getSchemaName)
      List(tableId)
    else List.empty[TableId]
  }.asJava

  override def getTableSchema(tableId: TableId): Schema = {
    PhakerDatabase.genSchema
  }
}
