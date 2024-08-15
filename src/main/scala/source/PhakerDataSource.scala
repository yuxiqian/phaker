package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.event.TableId
import org.apache.flink.cdc.common.source.{DataSource, EventSourceProvider, FlinkSourceFunctionProvider, MetadataAccessor}

class PhakerDataSource(
    tableId: TableId,
    rejectedTypes: Set[String],
    schemaEvolve: Boolean,
    maxColumnCount: Int,
    recordsPerSecond: Int
) extends DataSource {
  override def getEventSourceProvider: EventSourceProvider = {
    FlinkSourceFunctionProvider.of(
      new PhakerSourceFunction(
        new PhakerSourceGenerator(
          tableId,
          rejectedTypes,
          schemaEvolve,
          maxColumnCount
        ),
        recordsPerSecond
      )
    )
  }

  override def getMetadataAccessor: MetadataAccessor = {
    new PhakerMetadataAccessor(tableId)
  }
}
