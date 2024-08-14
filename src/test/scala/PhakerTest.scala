package io.github.yuxiqian.phaker

import factory.PhakerDataFactory
import source.PhakerSourceFunction

import org.apache.flink.cdc.common.event.TableId
import org.apache.flink.cdc.composer.definition.{SinkDef, SourceDef}
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory
import org.apache.flink.cdc.connectors.values.sink.{ValuesDataSink, ValuesDataSinkOptions}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class PhakerTest extends AnyFunSuite {

  import org.apache.flink.cdc.common.configuration.Configuration
  import org.apache.flink.cdc.common.pipeline.PipelineOptions
  import org.apache.flink.cdc.composer.definition.PipelineDef

  import java.util.Collections

  test("Phaker source test") {
    val source = new PhakerSourceFunction(
      TableId.tableId("default_namespace", "default_schema", "default_table"),
      true,
      17,
      17,
      1000
    )
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(source).print().setParallelism(1)
    env.execute("Let's Test Phaker Source...")
  }

  test("Phaker to Values test") {
    import source.PhakerDataSourceOptions

    import org.apache.flink.cdc.composer.definition.{RouteDef, TransformDef}

    val composer = FlinkPipelineComposer.ofMiniCluster

    // Setup value source
    val sourceConfig = new Configuration
    sourceConfig
      .set(PhakerDataSourceOptions.NAMESPACE_NAME, "default_namespace")
      .set(PhakerDataSourceOptions.SCHEMA_NAME, "default_schema")
      .set(PhakerDataSourceOptions.TABLE_NAME, "default_table")
      .set[java.lang.Integer](PhakerDataSourceOptions.BATCH_COUNT, 1)
      .set[java.lang.Integer](PhakerDataSourceOptions.MAX_COLUMN_COUNT, 50)
      .set[java.lang.Integer](PhakerDataSourceOptions.SLEEP_TIME, 1000)

    val sourceDef =
      new SourceDef(PhakerDataFactory.IDENTIFIER, "Value Source", sourceConfig)

    // Setup value sink
    val sinkConfig = new Configuration
    sinkConfig.set(
      ValuesDataSinkOptions.SINK_API,
      ValuesDataSink.SinkApi.SINK_V2
    )
    val sinkDef =
      new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig)

    // Setup pipeline
    val pipelineConfig = new Configuration
    pipelineConfig
      .set[java.lang.Integer](PipelineOptions.PIPELINE_PARALLELISM, 1)
    val pipelineDef = new PipelineDef(
      sourceDef,
      sinkDef,
      Collections.emptyList[RouteDef],
      Collections.emptyList[TransformDef],
      pipelineConfig
    )

    // Execute the pipeline
    val execution = composer.compose(pipelineDef)
    execution.execute
  }
}
