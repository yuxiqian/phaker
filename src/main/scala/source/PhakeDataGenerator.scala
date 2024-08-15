package io.github.yuxiqian.phaker
package source

import source.PhakerDatabase.idCount

import org.apache.flink.cdc.common.data._
import org.apache.flink.cdc.common.data.binary.BinaryStringData
import org.apache.flink.cdc.common.types._

import java.time.{Instant, ZonedDateTime}
import scala.util.Random

object PhakeDataGenerator {
  def randomType(rejectedTypes: Set[String]): DataType = {
    val choices = List(
      DataTypes.BINARY(17 + Random.nextInt(100)),
      DataTypes.VARBINARY(17 + Random.nextInt(100)),
      DataTypes.BOOLEAN,
      DataTypes.TINYINT,
      DataTypes.SMALLINT,
      DataTypes.INT,
      DataTypes.BIGINT,
      DataTypes.FLOAT,
      DataTypes.DOUBLE,
      DataTypes.CHAR(17 + Random.nextInt(100)),
      DataTypes.VARCHAR(17 + Random.nextInt(100)),
      DataTypes.DECIMAL(9 + Random.nextInt(8), Random.nextInt(8)),
      DataTypes.TIME(Random.nextInt(10)),
      DataTypes.TIMESTAMP(Random.nextInt(10)),
      DataTypes.TIMESTAMP_TZ(Random.nextInt(10)),
      DataTypes.TIMESTAMP_LTZ(Random.nextInt(10))
    ).filterNot(t => rejectedTypes.contains(t.getClass.getSimpleName))
    choices(Random.nextInt(choices.length))
  }

  def randomData(name: String, dataType: DataType): AnyRef = {
    if (name == PhakerDatabase.primaryKey) {
      return idCount
        .synchronized {
          idCount += 1;
          idCount
        }
        .asInstanceOf[AnyRef]
    }

    (dataType match {
      case binary: BinaryType       => generateBinary(binary.getLength)
      case varBinary: VarBinaryType => generateBinary(varBinary.getLength)
      case _: BooleanType           => generateBoolean()
      case _: TinyIntType           => generateTinyInt()
      case _: SmallIntType          => generateSmallInt()
      case _: IntType               => generateInt()
      case _: BigIntType            => generateBigInt()
      case _: FloatType             => generateFloat()
      case _: DoubleType            => generateDouble()
      case char: CharType           => generateString(char.getLength)
      case varChar: VarCharType     => generateString(varChar.getLength)
      case decimal: DecimalType =>
        generateDecimal(decimal.getPrecision, decimal.getScale)
      case _: TimeType              => generateTime()
      case timestamp: TimestampType => generateTimestamp(timestamp.getPrecision)
      case zonedTimestamp: ZonedTimestampType =>
        generateZonedTimestamp(zonedTimestamp.getPrecision)
      case localZonedTimestamp: LocalZonedTimestampType =>
        generateLocalZonedTimestamp(localZonedTimestamp.getPrecision)
    }).asInstanceOf[AnyRef]
  }

  private def generateBinary(length: Int): Array[Byte] = {
    val bytes = Array.fill[Byte](Random.nextInt(length))(0)
    Random.nextBytes(bytes)
    bytes
  }

  private def generateBoolean(): Boolean = {
    Random.nextBoolean
  }

  private def generateTinyInt(): Byte = {
    Random.nextInt.toByte
  }

  private def generateSmallInt(): Short = {
    Random.nextInt.toShort
  }

  private def generateInt(): Int = {
    Random.nextInt
  }

  private def generateBigInt(): Long = {
    Random.nextLong
  }

  private def generateFloat(): Float = {
    Random.nextFloat
  }

  private def generateDouble(): Double = {
    Random.nextDouble
  }

  private def generateString(length: Int): StringData = {
    BinaryStringData.fromString(Random.nextString(Random.nextInt(length)))
  }

  private def generateDecimal(precision: Int, scale: Int): DecimalData = {
    val maxValue = Math.pow(10, precision - 1).toInt
    DecimalData.fromUnscaledLong(Random.nextInt(maxValue), precision, scale)
  }

  private def generateTime(): Int = {
    System.currentTimeMillis.toInt
  }

  private def generateTimestamp(precision: Int): TimestampData = {
    TimestampData.fromMillis(System.currentTimeMillis)
  }

  private def generateZonedTimestamp(precision: Int): ZonedTimestampData = {
    ZonedTimestampData.fromZonedDateTime(ZonedDateTime.now)
  }

  private def generateLocalZonedTimestamp(
      precision: Int
  ): LocalZonedTimestampData = {
    LocalZonedTimestampData.fromInstant(Instant.now)
  }
}
