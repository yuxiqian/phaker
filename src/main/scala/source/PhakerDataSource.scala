package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.event.TableId
import org.apache.flink.cdc.common.source.{DataSource, EventSourceProvider, FlinkSourceFunctionProvider, MetadataAccessor}

class PhakerDataSource(
    tableId: TableId,
    rejectedTypes: Set[String],
    schemaEvolve: Boolean,
    generateNonNullColumns: Boolean,
    maxColumnCount: Int,
    recordsPerSecond: Int
) extends DataSource {
  override def getEventSourceProvider: EventSourceProvider = {
    FlinkSourceFunctionProvider.of(
      new PhakerSourceFunction(
        tableId,
        rejectedTypes,
        schemaEvolve,
        generateNonNullColumns,
        maxColumnCount,
        recordsPerSecond
      )
    )
  }

  override def getMetadataAccessor: MetadataAccessor = {
    new PhakerMetadataAccessor(tableId)
  }
}
