package org.ada.server.services.importers

import java.nio.charset.{Charset, MalformedInputException, UnsupportedCharsetException}

import javax.inject.Inject
import org.ada.server.models.dataimport.DataSetImport
import org.ada.server.AdaParseException
import org.ada.server.field.FieldTypeHelper
import org.ada.server.models._
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.dataset.{DataSetAccessor, DataSetAccessorFactory}
import org.ada.server.services.DataSetService
import org.ada.server.util.MessageLogger
import org.ada.server.field.FieldUtil.specToField
import org.ada.server.field.inference.FieldTypeInferrer
import org.incal.core.runnables.InputFutureRunnable
import org.incal.core.util.seqFutures
import play.api.libs.json.{JsNumber, JsObject, Json}
import play.api.Logger

import scala.concurrent.Future
import scala.io.Source
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import scala.concurrent.ExecutionContext.Implicits.global

trait DataSetImporter[T <: DataSetImport] extends InputFutureRunnable[T]

private[importers] abstract class AbstractDataSetImporter[T <: DataSetImport](implicit val typeTag: TypeTag[T]) extends DataSetImporter[T] {

  @Inject var messageRepo: MessageRepo = _
  @Inject var dataSetService: DataSetService = _
  @Inject var dsaf: DataSetAccessorFactory = _

  protected val logger = Logger

  protected val defaultFti = FieldTypeHelper.fieldTypeInferrer
  protected val ftf = FieldTypeHelper.fieldTypeFactory()
  protected val defaultCharset = "UTF-8"

  protected def createDataSetAccessor(
    importInfo: DataSetImport
  ): Future[DataSetAccessor] =
    for {
      dsa <- dsaf.register(
        importInfo.dataSpaceName,
        importInfo.dataSetId,
        importInfo.dataSetName,
        importInfo.setting,
        importInfo.dataView
      )

      _ <- dsa.updateDataSetRepo
    } yield
      dsa

  protected def createJsonsWithFields(
    fieldNamesAndLabels: Seq[(String, String)],
    values: Seq[Seq[String]],
    fti: Option[FieldTypeInferrer[String]] = None
  ): (Seq[JsObject], Seq[Field]) = {
    // infer field types
    val fieldTypes = values.transpose.par.map(fti.getOrElse(defaultFti).apply).toList

    // create jsons
    val jsons = values.map( vals =>
      JsObject(
        (fieldNamesAndLabels, fieldTypes, vals).zipped.map {
          case ((fieldName, _), fieldType, text) =>
            val jsonValue = fieldType.displayStringToJson(text)
            (fieldName, jsonValue)
        })
    )

    // create fields
    val fields = fieldNamesAndLabels.zip(fieldTypes).map { case ((name, label), fieldType) =>
      specToField(name, Some(label), fieldType.spec)
    }

    (jsons, fields)
  }

  protected def createJsonsWithStringFields(
    fieldNamesAndLabels: Seq[(String, String)],
    values: Iterator[Seq[String]]
  ): (Iterator[JsObject], Seq[Field]) = {
    // use String types for all the fields
    val fieldTypes = fieldNamesAndLabels.map(_ => ftf.stringScalar)

    // create jsons
    val jsons = values.map( vals =>
      JsObject(
        (fieldNamesAndLabels, fieldTypes, vals).zipped.map {
          case ((fieldName, _), fieldType, text) =>
            val jsonValue = fieldType.displayStringToJson(text)
            (fieldName, jsonValue)
        })
    )

    // create fields
    val fields = fieldNamesAndLabels.zip(fieldTypes).map { case ((name, label), fieldType) =>
      specToField(name, Some(label), fieldType.spec)
    }

    (jsons, fields)
  }

  protected def createCsvFileLineIterator(
    path: String,
    charsetName: Option[String],
    eol: Option[String]
  ): Iterator[String] = {
    def createSource = {
      val charset = Charset.forName(charsetName.getOrElse(defaultCharset))
      Source.fromFile(path)(charset)
    }

    try {
      eol match {
        case Some(eol) => {
          // TODO: not effective... if a custom eol is used we need to read the whole file into memory and split again. It'd be better to use a custom BufferedReader
          createSource.mkString.split(eol).iterator
        }
        case None =>
          createSource.getLines
      }
    } catch {
      case e: UnsupportedCharsetException => throw AdaParseException(s"Unsupported charset '${charsetName.get}' detected.", e)
      case e: MalformedInputException => throw AdaParseException("Malformed input detected. It's most likely due to some special characters. Try a different chartset.", e)
    }
  }

  protected def saveStringsAndDictionaryWithoutTypeInference(
    dsa: DataSetAccessor,
    columnNamesAndLabels: Seq[(String, String)],
    values: Iterator[Seq[String]],
    saveBatchSize: Option[Int]
  ): Future[Unit] = {
    // create jsons and field types
    logger.info(s"Creating JSONs...")
    val (jsons, fields) = createJsonsWithStringFields(columnNamesAndLabels, values)

    for {
      // save, or update the dictionary
      _ <- dataSetService.updateFields(dsa.fieldRepo, fields, true, true)

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- dsa.updateDataSetRepo

      // get the new data set repo
      dataRepo = dsa.dataSetRepo

      // remove ALL the records from the collection
      _ <- {
        logger.info(s"Deleting the old data set...")
        dsa.dataSetRepo.deleteAll
      }

      // save the jsons
      _ <- {
        logger.info(s"Saving JSONs...")
        saveBatchSize match {
          case Some(saveBatchSize) =>
            seqFutures(
              jsons.grouped(saveBatchSize))(
              dataRepo.save
            )

          case None =>
            Future.sequence(
              jsons.map(dataRepo.save)
            )
        }
      }
    } yield
      ()
  }

  protected def saveStringsAndDictionaryWithTypeInference(
    dsa: DataSetAccessor,
    fieldNamesAndLabels: Seq[(String, String)],
    values: Iterator[Seq[String]],
    saveBatchSize: Option[Int] = None,
    fti: Option[FieldTypeInferrer[String]] = None
  ): Future[Unit] = {
    // infer field types and create JSONSs
    logger.info(s"Inferring field types and creating JSONs...")

    val fieldNames = fieldNamesAndLabels.map(_._1)
    val (jsons, fields) = createJsonsWithFields(fieldNamesAndLabels, values.toSeq, fti)

    saveJsonsAndDictionary(dsa, jsons, fields, saveBatchSize)
  }

  protected def saveJsonsAndDictionary(
    dsa: DataSetAccessor,
    jsons: Seq[JsObject],
    fields: Seq[Field],
    saveBatchSize: Option[Int] = None
  ): Future[Unit] =
    for {
      // save, or update the dictionary
      _ <- dataSetService.updateFields(dsa.fieldRepo, fields, true, true)

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- dsa.updateDataSetRepo

      // get the new data set repo
      dataRepo = dsa.dataSetRepo

      // remove ALL the records from the collection
      _ <- {
        logger.info(s"Deleting the old data set...")
        dataRepo.deleteAll
      }

      // save the jsons
      _ <- {
        logger.info(s"Saving JSONs...")
        dataSetService.saveOrUpdateRecords(dataRepo, jsons,  None, false, None, saveBatchSize)
      }
    } yield
      ()
}