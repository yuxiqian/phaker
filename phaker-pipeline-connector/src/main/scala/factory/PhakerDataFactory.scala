package io.github.yuxiqian.phaker
package factory

import source.PhakerDataSource
import source.PhakerDataSourceOptions._

import org.apache.flink.cdc.common.configuration.ConfigOption
import org.apache.flink.cdc.common.factories.{DataSourceFactory, Factory}
import org.apache.flink.cdc.common.source.DataSource

import java.util
import scala.collection.JavaConverters._

object PhakerDataFactory {
  val IDENTIFIER = "phaker"
}

class PhakerDataFactory extends DataSourceFactory {

  override def createDataSource(context: Factory.Context): DataSource = {
    val conf = context.getFactoryConfiguration
    new PhakerDataSource(
      conf.get(NAMESPACE_NAME),
      conf.get(SCHEMA_NAME),
      conf.get(TABLE_NAME),
      conf.get(SCHEMA_EVOLVE),
      conf.get(MAX_COLUMN_COUNT),
      conf.get(BATCH_COUNT),
      conf.get(SLEEP_TIME)
    )
  }

  override def identifier(): String = PhakerDataFactory.IDENTIFIER

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    Set[ConfigOption[_]](NAMESPACE_NAME, SCHEMA_NAME, TABLE_NAME).asJava
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    Set[ConfigOption[_]](SCHEMA_EVOLVE, MAX_COLUMN_COUNT, BATCH_COUNT, SLEEP_TIME).asJava
  }
}
