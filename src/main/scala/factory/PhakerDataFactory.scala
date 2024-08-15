package io.github.yuxiqian.phaker
package factory

import source.PhakerDataSource
import source.PhakerDataSourceOptions._

import org.apache.flink.cdc.common.configuration.ConfigOption
import org.apache.flink.cdc.common.event.TableId
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
      TableId.parse(conf.get(TABLE_ID)),
      conf.get(REJECTED_TYPES).split(',').toSet,
      conf.get(SCHEMA_EVOLVE),
      conf.get(MAX_COLUMN_COUNT),
      conf.get(RECORDS_PER_SECOND)
    )
  }

  override def identifier(): String = PhakerDataFactory.IDENTIFIER

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    Set[ConfigOption[_]](TABLE_ID).asJava
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    Set[ConfigOption[_]](
      REJECTED_TYPES,
      SCHEMA_EVOLVE,
      MAX_COLUMN_COUNT,
      RECORDS_PER_SECOND
    ).asJava
  }
}
