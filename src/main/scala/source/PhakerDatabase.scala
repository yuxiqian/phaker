package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.schema.{Column, Schema}
import org.apache.flink.cdc.common.types.{DataType, DataTypes}

import java.util
object PhakerDatabase {
  val primaryKey: String = "id"
  var columnList: Array[(String, DataType)] = Array(
    (primaryKey, DataTypes.BIGINT)
  )
  var idCount: Long = 0
  var colCount = 0

  def genSchema: Schema = {
    PhakerDatabase.synchronized {

      Schema
        .newBuilder()
        .setColumns(
          util.Arrays.asList(
            PhakerDatabase.columnList
              .map(col =>
                Column.physicalColumn(col._1, col._2).asInstanceOf[Column]
              ): _*
          )
        )
        .primaryKey(
          PhakerDatabase.primaryKey
        )
        .build()
    }
  }
}
