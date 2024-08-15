package io.github.yuxiqian.phaker
package source

import source.PhakerDatabase.{colCount, idCount}

import org.apache.flink.cdc.common.event._
import org.apache.flink.cdc.common.schema.Column
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util

class PhakerSourceFunction(
    tableId: TableId,
    rejectedTypes: Set[String],
    schemaEvolve: Boolean,
    maxColumnCount: Int,
    batchCount: Int,
    sleepTime: Int
) extends SourceFunction[Event] {

  private type Context = SourceFunction.SourceContext[Event]

  private var isRunning = true

  override def run(ctx: Context): Unit = {

    ctx.collect(
      new CreateTableEvent(
        tableId,
        PhakerDatabase.genSchema
      )
    )

    while (isRunning) {
      PhakerDatabase.synchronized {
        println("Emitting insert events...")
        emitInsertEvents(ctx, batchCount)
        emitSchemaEvolutionEvents(ctx)

        println("Emitting update events...")
        emitUpdateEvents(ctx, batchCount)
        emitSchemaEvolutionEvents(ctx)

        println("Emitting delete events...")
        emitDeleteEvents(ctx, batchCount)
        emitSchemaEvolutionEvents(ctx)
      }
      Thread.sleep(sleepTime)
    }
  }

  private def emitInsertEvents(ctx: Context, count: Int): Unit = {
    for (_ <- 0 until count) {
      val insertedData = genRecord()
      ctx.collect(
        DataChangeEvent.insertEvent(tableId, insertedData)
      )
    }
  }

  private def emitUpdateEvents(ctx: Context, count: Int): Unit = {
    for (_ <- 0 until count) {
      val updateBeforeData = genRecord()
      ctx.collect(
        DataChangeEvent.insertEvent(tableId, updateBeforeData)
      )

      idCount.synchronized {
        idCount -= 1
      }

      val updateAfterData = genRecord()
      ctx.collect(
        DataChangeEvent.updateEvent(tableId, updateBeforeData, updateAfterData)
      )
    }
  }

  private def emitDeleteEvents(ctx: Context, count: Int): Unit = {
    for (_ <- 0 until count) {
      val deleteBeforeData = genRecord()
      ctx.collect(
        DataChangeEvent.insertEvent(tableId, deleteBeforeData)
      )

      idCount.synchronized {
        idCount -= 1
      }

      ctx.collect(
        DataChangeEvent.deleteEvent(tableId, deleteBeforeData)
      )
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

  private def emitSchemaEvolutionEvents(ctx: Context): Unit = {

    if (!schemaEvolve) { return }
    if (colCount > maxColumnCount) {
      return
    }

    println("Emitting schema change events...")

    val addedColumnName = colCount.synchronized {
      colCount += 1
      s"column$colCount"
    }
    val addedColumnType = PhakeDataGenerator.randomType(rejectedTypes)

    PhakerDatabase.columnList.synchronized {

      PhakerDatabase.columnList :+= (addedColumnName, addedColumnType)
      ctx.collect(
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

    println(s"Done, new schema: ${PhakerDatabase.genSchema}")
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
