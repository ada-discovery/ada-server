package org.ada.server.runnables.core

import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing, Sink, Source}
import org.incal.core.akka.AkkaFileIO.headerAndFileSource
import org.incal.core.util.writeByteArrayStream

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FeatureMatrixIO {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  def saveSquare[T](
    matrix: Traversable[Seq[T]],
    fieldNames: Seq[String],
    fileName: String,
    asString: T => String,
    delimiter: String = ","
  ) = save[T](matrix, fieldNames, fieldNames, "featureName",  fileName, asString, delimiter)

  def save[T](
    matrix: Traversable[Seq[T]],
    rowNames: Seq[String],
    columnNames: Seq[String],
    idColumnName: String,
    fileName: String,
    asString: T => String,
    delimiter: String = ","
  ): Unit = {
    val fixedColumnNames = columnNames.map(_.replaceAllLiterally("u002e", "."))
    val fixedRowNames = rowNames.map(_.replaceAllLiterally("u002e", "."))

    val header = idColumnName + delimiter + fixedColumnNames.mkString(delimiter) + "\n"
    val headerBytes = header.getBytes(StandardCharsets.UTF_8)

    val rowBytesStream = (matrix.toStream, fixedRowNames).zipped.toStream.map { case (row, name) =>
      val rowValues = row.map(asString).mkString(delimiter)
      val rowContent = name + delimiter + rowValues + "\n"
      rowContent.getBytes(StandardCharsets.UTF_8)
    }

    val outputStream = Stream(headerBytes) #::: rowBytesStream

    writeByteArrayStream(outputStream, new java.io.File(fileName))
  }

  def savePlain[T](
    matrix: Traversable[Seq[T]],
    columnNames: Seq[String],
    fileName: String,
    asString: T => String,
    delimiter: String = ","
  ): Unit = {
    val fixedColumnNames = columnNames.map(_.replaceAllLiterally("u002e", "."))

    val header = fixedColumnNames.mkString(delimiter) + "\n"
    val headerBytes = header.getBytes(StandardCharsets.UTF_8)

    val rowBytesStream = matrix.toStream.map {row =>
      val rowValues = row.map(asString).mkString(delimiter)
      val rowContent = rowValues + "\n"
      rowContent.getBytes(StandardCharsets.UTF_8)
    }

    val outputStream = Stream(headerBytes) #::: rowBytesStream

    writeByteArrayStream(outputStream, new java.io.File(fileName))
  }

  def load(
    fileName: String,
    skipFirstColumnsOption: Option[Int] = None,
    delimiter: String = ","
  ): Future[(Source[Seq[Double], _], Seq[String])] =
    for {
      (arraySource, fieldNames) <- loadArray(fileName, skipFirstColumnsOption, delimiter)
    } yield {
      val source = arraySource.map(_.toSeq)
      (source, fieldNames)
    }

  def loadArray(
    fileName: String,
    skipFirstColumnsOption: Option[Int],
    delimiter: String = ","
  ): Future[(Source[Array[Double], _], Seq[String])] =
    for {
      (header, contentSource) <- headerAndFileSource(fileName)
    } yield {
      val skipFirstColumns = skipFirstColumnsOption.getOrElse(0)
      val fieldNames = header.split(delimiter, -1).toSeq.drop(skipFirstColumns).map(_.trim)

      val source = contentSource.map { line =>
        line.split(delimiter, -1).drop(skipFirstColumns).map(_.trim.toDouble)
      }
      (source, fieldNames)
    }

  def loadWithFirstIdColumn(
    fileName: String,
    delimiter: String = ","
  ): Future[(Source[(String, Seq[Double]), _], Seq[String])] =
    for {
      (arraySource, fieldNames) <- loadArrayWithFirstIdColumn(fileName, delimiter)
    } yield {
      val source = arraySource.map { case (id, array) =>  (id, array.toSeq) }
      (source, fieldNames)
    }

  def loadArrayWithFirstIdColumn(
    fileName: String,
    delimiter: String = ","
  ): Future[(Source[(String, Array[Double]), _], Seq[String])] =
    for {
      (header, contentSource) <- headerAndFileSource(fileName)
    } yield {
      val fieldNames = header.split(delimiter, -1).map(_.trim)

      val source = contentSource.map { line =>
        val parts = line.split(delimiter, -1)
        (parts(0).trim, parts.tail.map(_.trim.toDouble))
      }
      (source, fieldNames)
    }
}