package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.configuration.{ConfigOption, ConfigOptions}

import java.lang

object PhakerDataSourceOptions {
  val NAMESPACE_NAME: ConfigOption[lang.String] = ConfigOptions
    .key("namespace.name")
    .stringType()
    .noDefaultValue()
    .withDescription("Namespace name of the simulated table.")

  val SCHEMA_NAME: ConfigOption[lang.String] = ConfigOptions
    .key("schema.name")
    .stringType()
    .noDefaultValue()
    .withDescription("Schema name of the simulated table.")

  val TABLE_NAME: ConfigOption[lang.String] = ConfigOptions
    .key("table.name")
    .stringType()
    .noDefaultValue()
    .withDescription("Name of the simulated table.")

  val SCHEMA_EVOLVE: ConfigOption[lang.Boolean] = ConfigOptions
    .key("schema.evolve")
    .booleanType()
    .defaultValue(true)
    .withDescription(
      "Whether generate schema evolution events occasionally. Defaults to true."
    )

  val MAX_COLUMN_COUNT: ConfigOption[lang.Integer] = ConfigOptions
    .key("max.column.count")
    .intType()
    .defaultValue(50)
    .withDescription("Max added columns count. No schema evolution events will be generated if this limit has exceeded. Defaults to 50.")


  val BATCH_COUNT: ConfigOption[lang.Integer] = ConfigOptions
    .key("batch.count")
    .intType()
    .defaultValue(17)
    .withDescription("Data records to be generated per batch. Defaults to 17.")

  val SLEEP_TIME: ConfigOption[lang.Integer] = ConfigOptions
    .key("sleep.time")
    .intType()
    .defaultValue(1000)
    .withDescription("Sleep time for a while during each batch (in milliseconds). Defaults to 1000.")
}
