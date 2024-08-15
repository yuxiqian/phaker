package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.event.Event
import org.apache.flink.streaming.api.functions.source.datagen.{DataGenerator, DataGeneratorSource}

class PhakerSourceFunction(
    generator: DataGenerator[Event],
    rowsPerSecond: Long
) extends DataGeneratorSource[Event](
      generator,
      rowsPerSecond,
      null
    ) {}
