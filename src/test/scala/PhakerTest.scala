package io.github.yuxiqian.phaker

import source.PhakerSourceFunction

import org.apache.flink.cdc.common.event.TableId
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

class PhakerTest extends AnyFunSuite {

  test("Phaker test example") {
    val source = new PhakerSourceFunction(
      TableId.tableId("default_namespace", "default_schema", "default_table"),
      true,
      17,
      1,
      1000
    )
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(source).print().setParallelism(1)
    env.execute("Let's Test Phaker Source...")
  }
}
