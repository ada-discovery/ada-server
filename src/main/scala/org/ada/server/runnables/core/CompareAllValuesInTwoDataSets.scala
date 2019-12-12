package org.ada.server.runnables.core

import javax.inject.Inject
import akka.NotUsed
import akka.actor.ActorSystem
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.libs.json._
import org.incal.core.runnables.{InputFutureRunnable, InputFutureRunnableExt}
import org.ada.server.services.DataSetService
import org.ada.server.dataaccess.ignite.BinaryJsonUtil.getValueFromJson
import org.ada.server.models.DataSetFormattersAndIds.{FieldIdentity, JsObjectIdentity}
import org.ada.server.models.StorageType
import org.incal.core.dataaccess.AscSort
import org.incal.core.akka.AkkaStreamUtil.zipSources

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.runtime.universe.typeOf
import akka.stream.{ActorMaterializer, SourceShape}
import akka.stream.scaladsl.{GraphDSL, Sink, Source, Zip}
import play.api.Logger

class CompareAllValuesInTwoDataSets @Inject()(
    dsaf: DataSetAccessorFactory,
    dataSetService: DataSetService
  ) extends InputFutureRunnableExt[CompareAllValuesInTwoDataSetsSpec] {

  private val logger = Logger
  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  override def runAsFuture(spec: CompareAllValuesInTwoDataSetsSpec) = {
    val dsa1 = dsaf(spec.dataSetId1).get
    val dsa2 = dsaf(spec.dataSetId2).get

    for {
      // setting1
      setting1 <- dsa1.setting

      // get all the field names
      fieldNames <- dsa1.fieldRepo.find(
        sort = Seq(AscSort(FieldIdentity.name)),
        skip = spec.fieldsSkip,
        limit = spec.fieldsNum
      ).map(_.map(_.name).toSeq)

      // switch the storage type
      _ <- dsa1.updateDataSetRepo(setting1.copy(storageType = spec.storageType1))

      // new data set repo1
      dataSetRepo1 = dsa1.dataSetRepo

      // setting2
      setting2 <- dsa2.setting

      // switch the storage type
      _ <- dsa2.updateDataSetRepo(setting2.copy(storageType = spec.storageType2))

      // new data set repo2
      dataSetRepo2 = dsa2.dataSetRepo

      // stream1
      stream1 <- {
        if(spec.fieldsNum.isDefined)
          logger.info(s"Creating a stream for these fields ${fieldNames.mkString(",")}.")
        else
          logger.info(s"Creating a stream for all available fields.")

        dataSetRepo1.findAsStream(
          sort = Seq(AscSort(spec.keyFieldName)),
          projection = if(spec.fieldsNum.isDefined) fieldNames :+ spec.keyFieldName else Nil
        )
      }

      // stream2
      stream2 <- dataSetRepo2.findAsStream(
        sort = Seq(AscSort(spec.keyFieldName)),
        projection = if(spec.fieldsNum.isDefined) fieldNames :+ spec.keyFieldName else Nil
      )

      // paired stream
      pairedStream = zipSources(stream1, stream2)

      // compare the jsons one-by-one and return the number of errors
      errorCount <- pairedStream.map((compare(spec.keyFieldName)(_, _)).tupled).runWith(Sink.fold(0)(_+_))
    } yield
      if (errorCount > 0)
        logger.error(s"In total $errorCount errors were found during json data set comparison.")
  }

  // returns the number of errors
  private def compare(keyFieldName: String)(jsObject1: JsObject, jsObject2: JsObject): Int = {
    val key1 = getValueFromJson((jsObject1 \ keyFieldName).get)
    val key2 = getValueFromJson((jsObject2 \ keyFieldName).get)

    val nonNullFields1 = jsObject1.fields.filterNot(_._2.equals(JsNull))
    val nonNullFields2 = jsObject2.fields.filterNot(_._2.equals(JsNull))

    val fieldsNum1 = nonNullFields1.size
    val fieldsNum2 = nonNullFields2.size

    assert(fieldsNum1.equals(fieldsNum2), s"The number of non-null fields $fieldsNum1 vs $fieldsNum2 do not match.")
    assert(key1.equals(key2), s"Keys $key1 vs $key2 do not match.")

    val errors = nonNullFields1.sortBy(_._1).zip(nonNullFields2.sortBy(_._1)).map { case ((fieldName1, jsValue1), (fieldName2, jsValue2)) =>
      if (!fieldName1.equals(JsObjectIdentity.name)) {
        val value1 = getValueFromJson(jsValue1)
        val value2 = getValueFromJson(jsValue2)

        if (!fieldName1.equals(fieldName2) || !value1.equals(value2)) {
          logger.error(s"Field $fieldName1 with value $value1 doesn't equal the field $fieldName2 with value $value2 for the key $key1.")
          1
        } else 0
      } else 0
    }.sum
    if (errors == 0)
      logger.info("No error found in a row...")
    errors
  }
}

case class CompareAllValuesInTwoDataSetsSpec(
  dataSetId1: String,
  storageType1: StorageType.Value,
  dataSetId2: String,
  storageType2: StorageType.Value,
  keyFieldName: String,
  fieldsNum: Option[Int],
  fieldsSkip: Option[Int]
)