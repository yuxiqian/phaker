package io.github.yuxiqian.phaker
package source

import source.PhakerDatabase.{colCount, idCount}

import org.apache.flink.cdc.common.event._
import org.apache.flink.cdc.common.schema.Column
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator

import java.util

class PhakerSourceGenerator(
    tableId: TableId,
    rejectedTypes: Set[String],
    schemaEvolve: Boolean,
    maxColumnCount: Int
) extends RandomGenerator[Event] {

  private val cachedEvents: util.List[Event] = {
    val cache = new util.ArrayList[Event]
    cache.add(
      new CreateTableEvent(
        tableId,
        PhakerDatabase.genSchema
      )
    )
    cache
  }

  override def next(): Event = {
    cachedEvents.synchronized {
      if (cachedEvents.isEmpty) {
        pushEvents()
      }
      cachedEvents.remove(0)
    }
  }

  private def pushEvents(): Unit = {
    PhakerDatabase.synchronized {
      println("Emitting insert events...")

      {
        val insertedData = genRecord()
        cachedEvents.add(
          DataChangeEvent.insertEvent(tableId, insertedData)
        )
      }

      {
        val updateBeforeData = genRecord()
        cachedEvents.add(
          DataChangeEvent.insertEvent(tableId, updateBeforeData)
        )

        idCount.synchronized {
          idCount -= 1
        }

        val updateAfterData = genRecord()
        cachedEvents.add(
          DataChangeEvent.updateEvent(
            tableId,
            updateBeforeData,
            updateAfterData
          )
        )
      }

      {

        val deleteBeforeData = genRecord()
        cachedEvents.add(
          DataChangeEvent.insertEvent(tableId, deleteBeforeData)
        )

        idCount.synchronized {
          idCount -= 1
        }

        cachedEvents.add(
          DataChangeEvent.deleteEvent(tableId, deleteBeforeData)
        )
      }

      {
        emitSchemaEvolutionEvents().foreach(
          cachedEvents.add
        )
      }
    }
  }

  private def genRecord() = {
    val generator = new BinaryRecordDataGenerator(
      PhakerDatabase.columnList.map(_._2)
    )
    val rowData = PhakerDatabase.columnList
      .map(col => PhakeDataGenerator.randomData(col._1, col._2))

    println(s"Generated data record: ${rowData.mkString("Array(", ", ", ")")}")
    generator.generate(
      rowData
    )
  }

  private def emitSchemaEvolutionEvents(): Option[Event] = {

    if (!schemaEvolve) { return None }
    if (colCount > maxColumnCount) {
      return None
    }

    println("Emitting schema change events...")

    val addedColumnName = colCount.synchronized {
      colCount += 1
      s"column$colCount"
    }
    val addedColumnType = PhakeDataGenerator.randomType(rejectedTypes)

    PhakerDatabase.columnList.synchronized {
      PhakerDatabase.columnList :+= (addedColumnName, addedColumnType)
      println(s"Done, new schema: ${PhakerDatabase.genSchema}")
      Some(
        new AddColumnEvent(
          tableId,
          util.Arrays.asList(
            new AddColumnEvent.ColumnWithPosition(
              Column.physicalColumn(
                addedColumnName,
                addedColumnType
              )
            )
          )
        )
      )
    }
  }
}
