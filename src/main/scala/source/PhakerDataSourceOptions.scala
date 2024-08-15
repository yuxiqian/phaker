package io.github.yuxiqian.phaker
package source

import org.apache.flink.cdc.common.configuration.{ConfigOption, ConfigOptions}

import java.lang

object PhakerDataSourceOptions {
  val TABLE_ID: ConfigOption[lang.String] = ConfigOptions
    .key("table.id")
    .stringType()
    .noDefaultValue()
    .withDescription("Table ID of the simulated table.")

  val REJECTED_TYPES: ConfigOption[lang.String] = ConfigOptions
    .key("rejected.types")
    .stringType()
    .defaultValue("")
    .withDescription("Unwanted data types (). Separated with comma.")

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
    .withDescription(
      "Max added columns count. No schema evolution events will be generated if this limit has exceeded. Defaults to 50."
    )

  val RECORDS_PER_SECOND: ConfigOption[lang.Integer] = ConfigOptions
    .key("records.per.second")
    .intType()
    .defaultValue(60)
    .withDescription(
      "Data records to be generated each second. Defaults to 60."
    )
}
