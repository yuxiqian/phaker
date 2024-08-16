package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.event.{Event, TableId}
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource

class PhakerSourceFunction(
    tableId: TableId,
    rejectedTypes: Set[String],
    schemaEvolve: Boolean,
    generateNonNullColumns: Boolean,
    maxColumnCount: Int,
    rowsPerSecond: Long
) extends DataGeneratorSource[Event](
      new PhakerSourceGenerator(
        tableId,
        rejectedTypes,
        schemaEvolve,
        generateNonNullColumns,
        maxColumnCount
      ),
      rowsPerSecond,
      null
    ) {}
