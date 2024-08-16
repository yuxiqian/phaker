package io.github.yuxiqian.phaker
package source

import source.PhakerDatabase.{colCount, idCount}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.cdc.common.event._
import org.apache.flink.cdc.common.schema.Column
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator

import java.util

class PhakerSourceGenerator(
    tableId: TableId,
    rejectedTypes: Set[String],
    schemaEvolve: Boolean,
    generateNonNullColumns: Boolean,
    maxColumnCount: Int
) extends RandomGenerator[Event] {

  private val cachedEvents = new util.ArrayList[Event]();

  override def open(
      name: String,
      context: FunctionInitializationContext,
      runtimeContext: RuntimeContext
  ): Unit = {
    super.open(name, context, runtimeContext)
    if (!schemaEvolve) {
      PhakerDatabase.columnList.synchronized {
        PhakerDatabase.columnList ++=
          PhakeDataGenerator
            .possibleChoices(rejectedTypes)
            .zipWithIndex
            .map(t =>
              (s"column${t._2 + 1}_${t._1.getClass.getSimpleName}", t._1)
            )
      }
    }
    cachedEvents.add(
      new CreateTableEvent(
        tableId,
        PhakerDatabase.genSchema
      )
    )
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
      PhakerDatabase.columnList.synchronized {
        PhakerDatabase.columnList.map(_._2)
      }
    )
    val rowData = PhakerDatabase.columnList
      .map(col => PhakeDataGenerator.randomData(col._1, col._2))

    println(
      s"Generated data record (${rowData.length}): ${rowData.mkString("Array(", ", ", ")")}"
    )
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

    val addedColumnType =
      PhakeDataGenerator.randomType(rejectedTypes, generateNonNullColumns)

    val addedColumnName = colCount.synchronized {
      colCount += 1
      s"column${colCount}_${addedColumnType.getClass.getSimpleName}"
    }
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
