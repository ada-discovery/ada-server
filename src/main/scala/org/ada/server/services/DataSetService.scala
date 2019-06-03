package org.ada.server.services

import java.{util => ju}

import javax.inject.Inject
import org.ada.server.models.DataSetFormattersAndIds.{CategoryIdentity, FieldIdentity, JsObjectIdentity}
import org.ada.server.dataaccess.RepoTypes.{FieldRepo, JsonCrudRepo, JsonReadonlyRepo}
import org.ada.server.dataaccess.JsonReadonlyRepoExtra._
import org.ada.server.dataaccess.JsonCrudRepoExtra._
import org.ada.server.dataaccess.{JsonUtil, StreamSpec}
import org.ada.server.util.MessageLogger
import org.ada.server.field.FieldUtil.{fieldTypeOrdering, isNumeric}
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps}
import com.google.inject.ImplementedBy
import org.ada.server.models._
import org.ada.server.dataaccess.RepoTypes._
import org.ada.server.dataaccess.dataset.{DataSetAccessor, DataSetAccessorFactory}
import play.api.Logger
import play.api.libs.json.{JsObject, _}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import reactivemongo.bson.BSONObjectID
import org.incal.core.dataaccess.Criterion.Infix
import org.incal.core.dataaccess.{AscSort, Criterion}
import org.incal.core.util.{GroupMapList, crossProduct, nonAlphanumericToUnderscore, retry, seqFutures}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import play.api.Configuration
import org.ada.server.dataaccess.JsonUtil._
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import org.ada.server.{AdaException, AdaParseException}
import org.ada.server.models._
import org.ada.server.field.{FieldType, FieldTypeHelper, FieldTypeInferrer}
import org.ada.server.models.ml._
import org.ada.server.models._

import scala.collection.Set
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
import org.ada.server.calc.impl.{BasicStatsResult, MultiBasicStatsCalc}
import org.ada.server.models.datatrans._
import org.ada.server.services.ml.IOSeriesUtil

import scala.util.Random

@ImplementedBy(classOf[DataSetServiceImpl])
trait DataSetService {

  @Deprecated
  def inferDictionaryAndUpdateRecords(
    dataSetId: String,
    fieldGroupSize: Int,
    fieldTypeIdsToExclude: Traversable[FieldTypeId.Value] = Nil
  ): Future[Unit]

  def updateDictionaryFields(
    dataSetId: String,
    newFields: Traversable[Field],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit]

  def updateDictionaryFields(
    fieldRepo: FieldRepo,
    newFields: Traversable[Field],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit]

  def updateDictionary(
    dataSetId: String,
    fieldNameAndTypes: Traversable[(String, FieldTypeSpec)],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit]

  def updateDictionary(
    fieldRepo: FieldRepo,
    fieldNameAndTypes: Traversable[(String, FieldTypeSpec)],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit]

  def getColumnNameLabels(
    delimiter: String,
    lineIterator: Iterator[String]
  ): Seq[(String, String)]

  def parseLines(
    columnsCount: Int,
    lines: Iterator[String],
    delimiter: String,
    skipFirstLine: Boolean,
    prefixSuffixSeparators: Seq[(String, String)] = Nil
  ): Iterator[Seq[String]]

  def parseLine(
    delimiter: String,
    line: String,
    prefixSuffixSeparators: Seq[(String, String)] = Nil
  ): Seq[String]

  def saveOrUpdateRecords(
    dataRepo: JsonCrudRepo,
    jsons: Seq[JsObject],
    keyField: Option[String] = None,
    updateExisting: Boolean = false,
    transformJsons: Option[Seq[JsObject] => Future[(Seq[JsObject])]] = None,
    batchSize: Option[Int] = None
  ): Future[Unit]

  def deleteRecordsExcept(
    dataRepo: JsonCrudRepo,
    keyField: String,
    keyValues: Seq[_]
  ): Future[Unit]

  def translateDataAndDictionary(
    originalDataSetId: String,
    newDataSetId: String,
    newDataSetName: String,
    newDataSetSetting: Option[DataSetSetting],
    newDataView: Option[DataView],
    useTranslations: Boolean,
    removeNullColumns: Boolean,
    removeNullRows: Boolean
  ): Future[Unit]

  def translateDataAndDictionaryOptimal(
    originalDataSetId: String,
    newDataSetId: String,
    newDataSetName: String,
    newDataSetSetting: Option[DataSetSetting],
    newDataView: Option[DataView],
    saveBatchSize: Option[Int],
    inferenceGroupSize: Option[Int],
    inferenceGroupsInParallel: Option[Int],
    jsonFieldTypeInferrer: Option[FieldTypeInferrer[JsReadable]] = None
  ): Future[Unit]

  def translateData(
    originalDataSetId: String,
    newDataSetId: String,
    newDataSetName: String,
    newDataSetSetting: Option[DataSetSetting],
    newDataView: Option[DataView],
    saveBatchSize: Option[Int]
  ): Future[Unit]

  def register(
    sourceDsa: DataSetAccessor,
    newDataSetId: String,
    newDataSetName: String,
    newStorageType: StorageType.Value
  ): Future[DataSetAccessor]

  def saveDerivedDataSet(
    sourceDsa: DataSetAccessor,
    derivedDataSetSpec: ResultDataSetSpec,
    inputSource: Source[JsObject, _],
    fields: Traversable[Field],
    streamSpec: StreamSpec = StreamSpec(),
    saveViewsAndFiltersFlag: Boolean = true
  ): Future[Unit]

  def mergeDataSets(
    resultDataSetSpec: ResultDataSetSpec,
    dataSetIds: Seq[String],
    fieldNameMappings: Seq[Seq[String]]
  ): Future[Unit]

  def mergeDataSetsWoInference(
    sourceDataSetIds: Seq[String],
    fieldNameMappings: Seq[Seq[Option[String]]],
    addSourceDataSetId: Boolean,
    resultDataSetSpec: ResultDataSetSpec,
    streamSpec: StreamSpec
  ): Future[Unit]

  def mergeDataSetsFullyWoInference(
    sourceDataSetIds: Seq[String],
    addSourceDataSetId: Boolean,
    resultDataSetSpec: ResultDataSetSpec,
    streamSpec: StreamSpec
  ): Future[Unit]

  def linkDataSets(
    spec: DataSetLinkSpec
  ) = linkMultiDataSets(
    MultiDataSetLinkSpec(
      spec.leftSourceDataSetId,
      Seq(spec.rightSourceDataSetId),
      spec.leftLinkFieldNames,
      Seq(spec.rightLinkFieldNames),
      spec.leftPreserveFieldNames,
      Seq(spec.rightPreserveFieldNames),
      spec.addDataSetIdToRightFieldNames,
      spec.resultDataSetSpec,
      spec.processingBatchSize,
      spec.saveBatchSize,
      spec.backpressureBufferSize,
      spec.parallelism
    )
  )

  def linkMultiDataSets(
    spec: MultiDataSetLinkSpec
  ): Future[Unit]

  def processSeriesAndSaveDataSet(
    spec: DataSetSeriesProcessingSpec
  ): Future[Unit]

  def transformSeriesAndSaveDataSet(
    spec: DataSetSeriesTransformationSpec
  ): Future[Unit]

  def loadDataAndFields(
    dsa: DataSetAccessor,
    fieldNames: Seq[String] = Nil,
    criteria: Seq[Criterion[Any]] = Nil
  ): Future[(Traversable[JsObject], Seq[Field])]

  def copyToNewStorage(
    dataSetId: String,
    groupSize: Int,
    parallelism: Int,
    backpressureBufferSize: Int,
    saveDeltaOnly: Boolean,
    targetStorageType: StorageType.Value
  ): Future[Unit]

  def selfLink(
    spec: SelfLinkSpec
  ): Future[Unit]

  def matchGroups(
    dataSetId: String,
    derivedDataSetSpec: ResultDataSetSpec,
    criteria: Seq[Criterion[Any]],
    targetGroupFieldName: String,
    confoundingFieldNames: Seq[String],
    numericDistTolerance: Double,
    targetGroupDisplayStringRatios: Seq[(String, Int)]
  ): Future[Unit]

  def extractPeaks(
    series: Seq[Double],
    peakNum: Int,
    peakSelectionRatio: Option[Double]
  ): Option[Seq[Double]]
}

class DataSetServiceImpl @Inject()(
    dsaf: DataSetAccessorFactory,
    translationRepo: TranslationRepo,
    sparkApp: SparkApp,
    messageRepo: MessageRepo,
    statsService: StatsService,
    configuration: Configuration
  ) extends DataSetService {

  private val logger = Logger
  private val messageLogger = MessageLogger(logger, messageRepo)
  private val dataSetIdFieldName = JsObjectIdentity.name
  private val reportLineFreq = 0.1

  private val ftf = FieldTypeHelper.fieldTypeFactory()
  private val fti = FieldTypeHelper.fieldTypeInferrer
  private val jsonFti = FieldTypeHelper.jsonFieldTypeInferrer

  private val idName = JsObjectIdentity.name

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  private type CreateJsonsWithFieldTypes =
    (Seq[String], Seq[Seq[String]]) => (Seq[JsObject], Seq[FieldType[_]])

  override def saveOrUpdateRecords(
    dataRepo: JsonCrudRepo,
    jsons: Seq[JsObject],
    keyField: Option[String] = None,
    updateExisting: Boolean = false,
    transformJsons: Option[Seq[JsObject] => Future[(Seq[JsObject])]] = None,
    batchSize: Option[Int] = None
  ): Future[Unit] = {
    val size = jsons.size
    val reportLineSize = size * reportLineFreq

    // helper function to transform and save given json records
    def transformAndSaveAux(
      startIndex: Int)(
      jsonRecords: Seq[JsObject]
    ): Future[Unit] = {
      val transformedJsonsFuture = transformJsons match {
        case Some(transformJsons) => transformJsons(jsonRecords)
        // if no transformation provided do nothing
        case None => Future(jsonRecords)
      }

      transformedJsonsFuture.flatMap { transformedJsons =>
        if (transformedJsons.nonEmpty) {
          logger.info(s"Saving ${transformedJsons.size} records...")
        }
//        Future.sequence(
//          transformedJsons.zipWithIndex.map { case (json, index) =>
//            dataRepo.save(json).map(_ =>
//              logProgress(startIndex + index + 1, reportLineSize, size)
//            )
//          }
//        ).map(_ => ())
        retry(s"Data saving failed:", logger.warn(_), 3)(
          dataRepo.save(transformedJsons).map { _ =>
            logProgress(startIndex + transformedJsons.size, reportLineSize, size)
            // TODO: flush??
            dataRepo.flushOps
          }
        )
      }
    }

    // helper function to transform and update given json records with key-matched existing records
    def transformAndUpdateAux(
      startIndex: Int)(
      jsonsWithIds: Seq[(JsObject, Traversable[JsValue])]
    ): Future[Unit] = {
      for {
        transformedJsonWithIds <-
          transformJsons match {
            case Some(transformJsons) =>
              for {
                transformedJsons <- transformJsons(jsonsWithIds.map(_._1))
              } yield {
                transformedJsons.zip(jsonsWithIds).map { case (transformedJson, (_, ids)) =>
                  (transformedJson, ids)
                }
              }
            // if no transformation provided do nothing
            case None => Future(jsonsWithIds)
          }
        _ <- {
          if (transformedJsonWithIds.nonEmpty) {
            logger.info(s"Updating ${transformedJsonWithIds.size} records...")
          }
          Future.sequence(
            transformedJsonWithIds.zipWithIndex.map { case ((json, ids), index) =>
              Future.sequence(
                ids.map { id =>
                  dataRepo.update(json.+(dataSetIdFieldName, id)).map(_ =>
                    logProgress(startIndex + index + 1, reportLineSize, size)
                  )
                }
              )
            }
          ).map(_.flatten)
        }
      } yield
        ()
    }

    // helper function to transform and save or update given json records
    def transformAndSaveOrUdateAux(
      startIndex: Int)(
      jsonRecords: Seq[(JsObject, JsValue)]
    ): Future[Unit] = {
      val keys = jsonRecords.map(_._2)

      val jsonsWithIdsFuture: Future[Seq[(JsObject, Traversable[JsValue])]] =
        for {
          keyIds <- dataRepo.find(
            criteria = Seq(keyField.get #-> keys),
            projection = Seq(keyField.get, dataSetIdFieldName)
          )
        } yield {
          // create a map of key-ids pairs
          val keyIdMap: Map[JsValue, Traversable[JsValue]] = keyIds.map { keyIdJson =>
            val key = (keyIdJson \ keyField.get).get
            val id = (keyIdJson \ dataSetIdFieldName).get
            (key, id)
          }.groupBy(_._1).map{ case (key, keyAndIds) => (key, keyAndIds.map(_._2))}

          jsonRecords.map { case (json, key) =>
            (json, keyIdMap.get(key).getOrElse(Nil))
          }
        }

      jsonsWithIdsFuture.flatMap { jsonsWithIds =>
        val jsonsToSave = jsonsWithIds.filter(_._2.isEmpty).map(_._1)
        val jsonsToUpdate = jsonsWithIds.filter(_._2.nonEmpty)

        for {
          _ <- if (updateExisting) {
            // update the existing records (if requested)
            transformAndUpdateAux(startIndex + jsonsToSave.size)(jsonsToUpdate)
          } else {
            if (jsonsToUpdate.nonEmpty) {
              logger.info(s"Records already exist. Skipping...")
              // otherwise do nothing... the source records are expected to be readonly
              for (index <- 0 until jsonsToUpdate.size) {
                logProgress(startIndex + jsonsToSave.size + index + 1, reportLineSize, size)
              }
            }
            Future(Nil)
          }
          // save the new records
          _ <- transformAndSaveAux(startIndex)(jsonsToSave)
        } yield ()
      }
    }

    // helper function to transform and save or update given json records
    def transformAndSaveOrUpdateMainAux(startIndex: Int)(jsonRecords: Seq[JsObject]): Future[Unit] =
      if (keyField.isDefined) {
        val jsonKeyPairs = jsonRecords.map(json => (json, (json \ keyField.get).toOption))
        val jsonsWithKeys: Seq[(JsObject, JsValue)] = jsonKeyPairs.filter(_._2.isDefined).map(x => (x._1, x._2.get))
        val jsonsWoKeys: Seq[JsObject] = jsonKeyPairs.filter(_._2.isEmpty).map(_._1)

        // if no key found transform and save
        transformAndSaveAux(startIndex)(jsonsWoKeys)

        // if key is found update or save
        transformAndSaveOrUdateAux(startIndex + jsonsWoKeys.size)(jsonsWithKeys)
      } else {
        // no key field defined, perform pure save
        transformAndSaveAux(startIndex)(jsonRecords)
      }

    ///////////////
    // Main part //
    ///////////////

    if (batchSize.isDefined) {
      val indexedGroups = jsons.grouped(batchSize.get).zipWithIndex

      indexedGroups.foldLeft(Future(())){
        case (x, (groupedJsons, groupIndex)) =>
          x.flatMap {_ =>
            transformAndSaveOrUpdateMainAux(groupIndex * batchSize.get)(groupedJsons)
          }
        }
    } else
      // save all the records
      transformAndSaveOrUpdateMainAux(0)(jsons)
  }

  override def deleteRecordsExcept(
    dataRepo: JsonCrudRepo,
    keyField: String,
    keyValues: Seq[_]
  ) =
    for {
      recordsToRemove <- dataRepo.find(
        criteria = Seq(keyField #!-> keyValues),
        projection = Seq(dataSetIdFieldName)
      )

      _ <- {
        if (recordsToRemove.nonEmpty) {
          logger.info(s"Deleting ${recordsToRemove.size} (old) records not contained in the newly imported data set.")
        }
        Future.sequence(
          recordsToRemove.map(recordToRemove =>
            dataRepo.delete((recordToRemove \ dataSetIdFieldName).get.as[BSONObjectID])
          )
        )
      }
    } yield
      ()

  override def parseLines(
    columnCount: Int,
    lines: Iterator[String],
    delimiter: String,
    skipFirstLine: Boolean,
    prefixSuffixSeparators: Seq[(String, String)] = Nil
  ): Iterator[Seq[String]] = {
    val contentLines = if (skipFirstLine) lines.drop(1) else lines

    val lineBuffer = ListBuffer[String]()

    // helper function to parse a line and handle a parse exception by returning None
    def parse(line: String): Option[Seq[String]] =
      try {
        val values =
          if (lineBuffer.isEmpty) {
            parseLine(delimiter, line, prefixSuffixSeparators)
          } else {
            val bufferedLine = lineBuffer.mkString("") + line
            parseLine(delimiter, bufferedLine, prefixSuffixSeparators)
          }
        Some(values)
      } catch {
        case e: AdaParseException => None
      }

    // read all the lines
    contentLines.zipWithIndex.flatMap { case (line, index) =>
      // parse the line
      val values = parse(line)

      if (values.isEmpty) {
        logger.info(s"Buffered line ${index} could not be parse due to the unmatched prefix and suffix $prefixSuffixSeparators. Buffering...")
        lineBuffer.+=(line)
        Option.empty[Seq[String]]
      } else if (values.get.size < columnCount) {
        logger.info(s"Buffered line ${index} has an unexpected count '${values.get.size}' vs '${columnCount}'. Buffering...")
        lineBuffer.+=(line)
        Option.empty[Seq[String]]
      } else if (values.get.size > columnCount) {
        throw new AdaParseException(s"Buffered line ${index} has overflown an unexpected count '${values.get.size}' vs '${columnCount}'. Parsing terminated. Line: ${line}. Parsed values:\n ${values.get.mkString("\n")}")
      } else {
        // reset the buffer
        lineBuffer.clear()
        values
      }
    }
  }

  @Deprecated
  override def inferDictionaryAndUpdateRecords(
    dataSetId: String,
    fieldGroupSize: Int,
    fieldTypeIdsToExclude: Traversable[FieldTypeId.Value]
  ): Future[Unit] = {
    logger.info(s"Dictionary inference for data set '${dataSetId}' initiated.")

    val dsa = dsaf(dataSetId).get
    val dataRepo = dsa.dataSetRepo
    val fieldRepo = dsa.fieldRepo

    // helper functions to parse jsons
    def displayJsonToJson[T](fieldType: FieldType[T], json: JsReadable): JsValue = {
      val value = fieldType.displayJsonToValue(json)
      fieldType.valueToJson(value)
    }

    for {
      // get the fields to process
      fields <-
        fieldRepo.find(Seq("fieldType" #!-> fieldTypeIdsToExclude.map(_.toString).toSeq))

      // infer field types
      fieldNameAndTypes <-
        inferFieldTypesInParallel(dataRepo, fields.map(_.name), fieldGroupSize)

      // get all the items
      items <- dataRepo.find()

      // save or update the items
      _ <- {
        val newItems  = items.map { item =>
          val fieldNameJsons = fieldNameAndTypes.map { case (fieldName, fieldType) =>
            val newJson = displayJsonToJson(fieldType, (item \ fieldName))
            (fieldName, newJson)
          }
          item ++ JsObject(fieldNameJsons.toSeq)
        }

        saveOrUpdateRecords(dataRepo, newItems.toSeq, Some(dataSetIdFieldName), true)
      }

      // save, update, or delete the fields
      _ <- {
        val fieldNameAndTypeSpecs = fieldNameAndTypes.map { case (fieldName, fieldType) => (fieldName, fieldType.spec)}
        updateDictionary(fieldRepo, fieldNameAndTypeSpecs, false, false)
      }
    } yield
      messageLogger.info(s"Dictionary inference for data set '${dataSetId}' successfully finished.")
  }

  private def getFieldNames(dataRepo: JsonCrudRepo): Future[Set[String]] =
    for {
      records <- dataRepo.find(limit = Some(1))
    } yield
      records.headOption.map(_.keys).getOrElse(
        throw new AdaException(s"No records found. Unable to obtain field names. The associated data set might be empty.")
      )

  private def inferFieldTypesInParallel(
    dataRepo: JsonCrudRepo,
    fieldNames: Traversable[String],
    groupSize: Int,
    jsonFieldTypeInferrer: Option[FieldTypeInferrer[JsReadable]] = None
  ): Future[Traversable[(String, FieldType[_])]] = {
    val groupedFieldNames = fieldNames.toSeq.grouped(groupSize).toSeq
    val jfti = jsonFieldTypeInferrer.getOrElse(jsonFti)

    for {
      fieldNameAndTypes <- Future.sequence(
        groupedFieldNames.par.map { groupFieldNames =>
          dataRepo.find(projection = groupFieldNames).map(
            inferFieldTypes(jfti, groupFieldNames)
          )
        }.toList
      )
    } yield
      fieldNameAndTypes.flatten
  }

  override def mergeDataSets(
    resultDataSetSpec: ResultDataSetSpec,
    dataSetIds: Seq[String],
    fieldNames: Seq[Seq[String]]
  ): Future[Unit] = {
    val dsafs = dataSetIds.map(dsaf(_).get)
    val dataSetRepos = dsafs.map(_.dataSetRepo)
    val fieldRepos = dsafs.map(_.fieldRepo)
    val newFieldNames = fieldNames.map(_.head)

    for {
      // register the result data set (if not registered already)
      newDsa <- registerDerivedDataSet(dsafs.head, resultDataSetSpec)

      newFieldRepo = newDsa.fieldRepo

      fields <- dsafs.head.fieldRepo.find(Seq("name" #-> newFieldNames))
      fieldNameMap = fields.map(field => (field.name, field)).toMap

      namedFieldTypes <- Future.sequence(
        fieldRepos.zipWithIndex.map { case (fieldRepo, index) =>
          val names = fieldNames.map(_(index))
          fieldRepo.find(Seq("name" #-> names)).map { fields =>
            val nameFieldMap = fields.map(field => (field.name, field)).toMap
            names.map(name =>
              (name, ftf(nameFieldMap.get(name).get.fieldTypeSpec))
            )
          }
        }
      )

      fieldTypesWithNewNames = newFieldNames.zip(namedFieldTypes.transpose).map { case (newFieldName, namedFieldTypes) =>
        (namedFieldTypes, newFieldName)
      }

      newFieldNameAndTypes <- inferMultiSourceFieldTypesInParallel(dataSetRepos, fieldTypesWithNewNames, 100, None)

      // delete all the new fields
      _ <- newFieldRepo.deleteAll

      // save the new fields
      _ <- {
        val newFields = newFieldNameAndTypes.map { case (fieldName, fieldType) =>
          val fieldTypeSpec = fieldType.spec
          val stringEnums = fieldTypeSpec.enumValues.map { case (from, to) => (from.toString, to) }

          fieldNameMap.get(fieldName).map( field =>
            Field(name = fieldName, label = field.label, fieldType = fieldTypeSpec.fieldType, isArray = fieldTypeSpec.isArray, enumValues = stringEnums)
          )
        }.flatten

        val dataSetIdEnumds = dataSetIds.zipWithIndex.map { case (dataSetId, index) => (index.toString, dataSetId) }.toMap
        val sourceDataSetIdField = Field("source_data_set_id", Some("Source Data Set Id"), FieldTypeId.Enum, false, dataSetIdEnumds)

        newFieldRepo.save(newFields ++ Seq(sourceDataSetIdField))
      }

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- newDsa.updateDataSetRepo

      // get the new data set repo
      newDataRepo = newDsa.dataSetRepo

      // delete all the data
      _ <- {
        logger.info(s"Deleting all the data for '${resultDataSetSpec.id}'.")
        newDataRepo.deleteAll
      }

      // save the new items
      _ <- {
        logger.info("Saving new items")
        val newFieldNameAndTypeMap: Map[String, FieldType[_]] = newFieldNameAndTypes.toMap

        Future.sequence(
         dataSetRepos.zipWithIndex.map { case (dataSetRepo, index) =>
            val fieldNewFieldNames: Seq[(String, (FieldType[_], String))] = fieldTypesWithNewNames.map { case (fields, newFieldName) =>
              (fields(index)._1, (fields(index)._2, newFieldName))
            }
            val fieldNewFieldNameMap = fieldNewFieldNames.toMap

            dataSetRepo.find(projection = fieldNewFieldNameMap.map(_._1)).map { jsons =>
              val newJsons = jsons.map { json =>
                val newFieldValues = json.fields.map { case (fieldName, jsValue) =>
                  val (fieldType, newFieldName) = fieldNewFieldNameMap.get(fieldName).get
                  val newFieldType = newFieldNameAndTypeMap.get(newFieldName).get
                  (newFieldName, newFieldType.displayStringToJson(fieldType.jsonToDisplayString(jsValue)))
                }
                JsObject(newFieldValues ++ Seq(("source_data_set_id", JsNumber(index))))
              }
              newDataRepo.save(newJsons)
            }
          }
        )
      }
    } yield
      ()
  }

  override def mergeDataSetsFullyWoInference(
    sourceDataSetIds: Seq[String],
    addSourceDataSetId: Boolean,
    resultDataSetSpec: ResultDataSetSpec,
    streamSpec: StreamSpec
  ): Future[Unit] = {
    val dsafs = sourceDataSetIds.map(dsaf(_).get)
    val fieldRepos = dsafs.map(_.fieldRepo)

    for {
      // collect all the field names
      allFieldNameSets <- Future.sequence(
        fieldRepos.map(
          _.find().map(
            _.map(_.name).toSet
          ))
        )

      // merge all field names
      allFieldNames = allFieldNameSets.flatten.toSet

      // create field name mappings
      fieldNameMappings = allFieldNames.map(fieldName =>
        allFieldNameSets.map(set =>
          if (set.contains(fieldName)) Some(fieldName) else None
        )
      ).toSeq

      // merge data sets
      _ <- mergeDataSetsWoInference(
        sourceDataSetIds,
        fieldNameMappings,
        addSourceDataSetId,
        resultDataSetSpec,
        streamSpec
      )
    } yield
      ()
  }

  override def mergeDataSetsWoInference(
    sourceDataSetIds: Seq[String],
    fieldNameMappings: Seq[Seq[Option[String]]],
    addSourceDataSetId: Boolean,
    resultDataSetSpec: ResultDataSetSpec,
    streamSpec: StreamSpec
  ): Future[Unit] = {
    val dsafs = sourceDataSetIds.map(dsaf(_).get)
    val dataSetRepos = dsafs.map(_.dataSetRepo)
    val fieldRepos = dsafs.map(_.fieldRepo)

    logger.info(s"Merging the data sets ${sourceDataSetIds.mkString(", ")} (without inference) using the ${fieldNameMappings.size} mappings \n: ${fieldNameMappings.map(_.mkString(",")).mkString("\n")}")

    for {
      // collect all the fields
      allFields <- Future.sequence(
        fieldRepos.zipWithIndex.map { case (fieldRepo, index) =>
          val names = fieldNameMappings.map(_(index))

          fieldRepo.find(Seq(FieldIdentity.name #-> names.flatten)).map { fields =>
            val nameFieldMap = fields.map(field => (field.name, field)).toMap

            names.map(_.flatMap(nameFieldMap.get))
          }
        }
      )

      // new fields
      newFields = allFields.transpose.map { case fields =>
        // check if all the field specs are the same
        def equalFieldTypes(field1: Field)(field2: Field): Boolean = {
          val enums1 = field1.enumValues.toSeq.sortBy(_._1)
          val enums2 = field2.enumValues.toSeq.sortBy(_._1)

          field1.fieldType == field2.fieldType &&
            field1.isArray == field2.isArray &&
            enums1.size == enums2.size &&
            enums1.zip(enums2).forall { case ((a1, b1), (a2, b2)) => a1.equals(a2) && b1.equals(b2) }
        }

        val nonEmptyFields = fields.flatten
        val headField = nonEmptyFields.head
        val equalFieldSpecTypes = nonEmptyFields.tail.forall(equalFieldTypes(headField))
        if (!equalFieldSpecTypes)
          throw new AdaException(s"The data types for the field ${headField.name} differ: ${nonEmptyFields.mkString(",")}")

        headField
      }

      // add source_data_id to the new fields (if needed)
      finalNewFields =
        if (addSourceDataSetId) {
          val dataSetIdEnums = sourceDataSetIds.zipWithIndex.map { case (dataSetId, index) => (index.toString, dataSetId) }.toMap
          val sourceDataSetIdField = Field("source_data_set_id", Some("Source Data Set Id"), FieldTypeId.Enum, false, dataSetIdEnums)

          newFields ++ Seq(sourceDataSetIdField)
        } else
          newFields

      // collect all the source streams
      streams <- seqFutures(dataSetRepos.zip(allFields).zipWithIndex) { case ((dataSetRepo, fields), index) =>

        // create a map of old to new field names
        val fieldNewFieldNameMap = fields.zip(newFields).flatMap { case (fieldOption, newField) =>
          fieldOption.map(field => (field.name, newField.name))
        }.toMap

        dataSetRepo.findAsStream().map { originalStream =>

          originalStream.map { json =>

            val newFieldValues = json.fields
              .filterNot(_._1.equals(JsObjectIdentity.name))
              .map { case (fieldName, jsValue) =>
                val newFieldName = fieldNewFieldNameMap.get(fieldName).getOrElse(throw new AdaException(s"Field $fieldName not found."))
                (newFieldName, jsValue)
              }

            val extraFieldValues = if (addSourceDataSetId) Seq(("source_data_set_id", JsNumber(index))) else Nil

            JsObject(newFieldValues ++ extraFieldValues)
          }
        }
      }

      // concatenate all the streams
      mergedStream = streams.tail.foldLeft(streams.head)(_.concat(_))

      // finally save the derived data sets with a given stream and fields
      _ <- saveDerivedDataSet(dsafs.head, resultDataSetSpec, mergedStream, finalNewFields, streamSpec, false)
    } yield
      ()
  }

  private def inferMultiSourceFieldTypesInParallel(
    dataRepos: Seq[JsonCrudRepo],
    fieldTypesWithNewNames: Traversable[(Seq[(String, FieldType[_])], String)],
    groupSize: Int,
    jsonFieldTypeInferrer: Option[FieldTypeInferrer[JsReadable]] = None
  ): Future[Traversable[(String, FieldType[_])]] = {
    val groupedFieldTypesWithNewNames = fieldTypesWithNewNames.toSeq.grouped(groupSize).toSeq
    val jfti = jsonFieldTypeInferrer.getOrElse(jsonFti)

    def jsonToDisplayJson[T](fieldType: FieldType[T], jsValue: JsValue): JsValue =
      fieldType.jsonToValue(jsValue).map(x =>
        JsString(fieldType.valueToDisplayString(Some(x)))
      ).getOrElse(JsNull)

    for {
      fieldNameAndTypes <- Future.sequence(
        groupedFieldTypesWithNewNames.par.map { groupFields =>
          Future.sequence(
            dataRepos.zipWithIndex.map { case (dataRepo, index) =>
              val fieldNewFieldNames: Seq[(String, (FieldType[_], String))] = groupFields.map { case (fields, newFieldName) =>
                (fields(index)._1, (fields(index)._2, newFieldName))
              }

              val fieldNewFieldNameMap = fieldNewFieldNames.toMap
              dataRepo.find(projection = fieldNewFieldNames.map(_._1)).map(_.map { json =>
                val jsonFields = json.fields.map { case (fieldName, jsValue) =>
                  val fieldTypeNewFieldName = fieldNewFieldNameMap.get(fieldName).get
                  val newFieldName = fieldTypeNewFieldName._2
                  val fieldType = fieldTypeNewFieldName._1
                  (newFieldName, jsonToDisplayJson(fieldType, jsValue))
                }
                JsObject(jsonFields)
              })
            }
          ).map { jsons =>
            val newFieldNames = fieldTypesWithNewNames.map(_._2)
            inferFieldTypes(jfti, newFieldNames)(jsons.flatten)
          }
        }.toList
      )
    } yield
      fieldNameAndTypes.flatten
  }

  case class LinkedDataSetInfo(
    dsa: DataSetAccessor,
    preserveFieldNames: Traversable[String],
    linkFieldNames: Seq[String]
  ) {
    lazy val fieldNamesToLoad: Traversable[String] = {
      preserveFieldNames match {
        case Nil => Nil
        case _ => (preserveFieldNames ++ linkFieldNames).toSet
      }
    }
  }

  override def linkMultiDataSets(
    spec: MultiDataSetLinkSpec
  ) = {
    val leftDataSetInfo = dsaf(spec.leftSourceDataSetId).map { dsa =>
      LinkedDataSetInfo(dsa, spec.leftPreserveFieldNames, spec.leftLinkFieldNames)
    }.getOrElse(throw new AdaException(s"Data Set ${spec.leftSourceDataSetId} not found."))

    val leftDsa = leftDataSetInfo.dsa

    val rightDataSetInfos = spec.rightSourceDataSetIds.zip(spec.rightPreserveFieldNames).zip(spec.rightLinkFieldNames).map {
      case ((dataSetId, preserveFieldNames), linkFieldNames) =>
        val dsa = dsaf(dataSetId).getOrElse(throw new AdaException(s"Data Set ${dataSetId} not found."))
        LinkedDataSetInfo(dsa, preserveFieldNames, linkFieldNames)
    }

    val processingBatchSize = spec.processingBatchSize.getOrElse(10)
    val bufferSize = spec.backpressureBufferSize.getOrElse(10)

    for {
      // register the result data set (if not registered already)
      linkedDsa <- registerDerivedDataSet(leftDataSetInfo.dsa, spec.resultDataSetSpec)

      // get all the left data set fields
      leftFieldTypeMap <- fieldTypeMap(leftDataSetInfo)

      // get all the right data set fields
      rightFieldTypeMaps <- Future.sequence(rightDataSetInfos.map(fieldTypeMap))

      // update the linked dictionary
      _ <- {
        val leftFieldNameAndTypes = leftFieldTypeMap.map { case (fieldName, fieldType) => (fieldName, fieldType.spec) }.toSeq

        val rightFieldNameAndTypesWoLink = rightFieldTypeMaps.zip(rightDataSetInfos).flatMap { case (rightFieldTypeMap, rightDataSetInfo) =>
          val linkFieldNameSet = rightDataSetInfo.linkFieldNames.toSet
          val fieldNameTypesWoLink = rightFieldTypeMap.map { case (fieldName, fieldType) => (fieldName, fieldType.spec)}.filterNot { case (fieldName, _) =>
            linkFieldNameSet.contains(fieldName)
          }.toSeq

          if (spec.addDataSetIdToRightFieldNames)
            fieldNameTypesWoLink.map { case (fieldName, fieldTypeSpec) =>
              (rightDataSetInfo.dsa.dataSetId.replace('.', '_') + "-" + fieldName, fieldTypeSpec)
            }
          else
            fieldNameTypesWoLink
        }

        updateDictionary(spec.resultDataSetId, leftFieldNameAndTypes ++ rightFieldNameAndTypesWoLink, false, true)
      }

      // the right data set link->jsons
      linkRightJsonsMaps <- Future.sequence(
        rightDataSetInfos.zip(rightFieldTypeMaps).map {
          case (rightDataSetInfo, rightFieldTypeMap) => linkJsonsMap(rightDataSetInfo, rightFieldTypeMap, spec.addDataSetIdToRightFieldNames)
        }
      )

      // aux function to link and save jsons
      linkAndSaveAux = linkAndSave(spec.leftLinkFieldNames, linkRightJsonsMaps, leftFieldTypeMap, linkedDsa, spec.saveBatchSize)(_)

      // delete all items from the linked data set
      _ <- linkedDsa.dataSetRepo.deleteAll

      // create an input stream for the left data set
      stream <- leftDsa.dataSetRepo.findAsStream(projection = leftDataSetInfo.fieldNamesToLoad)

      // group, link and save the stream as it goes
      _ <- stream
          .grouped(processingBatchSize)
          .buffer(bufferSize, OverflowStrategy.backpressure)
          .mapAsync(spec.parallelism.getOrElse(1))(linkAndSaveAux)
          .runWith(Sink.ignore)
    } yield
      ()
  }


  private def fieldTypeMap(
    dataSetInfo: LinkedDataSetInfo
  ): Future[Map[String, FieldType[_]]] = {
    val fieldRepo = dataSetInfo.dsa.fieldRepo
    val fieldNames = dataSetInfo.fieldNamesToLoad

    for {
      fields <- fieldNames match {
        case Nil => fieldRepo.find()
        case _ => fieldRepo.find(Seq(FieldIdentity.name #-> fieldNames.toSeq))
      }
    } yield
      fields.map(field => (field.name, ftf(field.fieldTypeSpec))).toMap
  }

  private def linkAndSave(
    leftLinkFieldNames: Seq[String],
    linkRightJsonsMaps: Seq[Map[Seq[String], Traversable[JsObject]]],
    leftFieldTypeMap: Map[String, FieldType[_]],
    linkedDsa: DataSetAccessor,
    saveBatchSize: Option[Int])(
    leftJsons: Traversable[JsObject]
  ): Future[Unit] = {
    val linkedJsons = leftJsons.flatMap { json =>
      val link = leftLinkFieldNames.map { fieldName =>
        val fieldType = leftFieldTypeMap.get(fieldName).getOrElse(
          throw new AdaException(s"Field $fieldName not found.")
        )
        fieldType.jsonToDisplayString(json \ fieldName)
      }

      val jsonId = (json \ JsObjectIdentity.name).asOpt[BSONObjectID]

      val rightJsonsCrossed = crossProduct(linkRightJsonsMaps.flatMap(_.get(link)))

      if (rightJsonsCrossed.isEmpty) {
        Seq(json)
      } else {
        rightJsonsCrossed.map { rightJsons =>
          val rightJson: JsObject = rightJsons.foldLeft(Json.obj()) {_ ++ _}
          val id = if (rightJsonsCrossed.size > 1 || jsonId.isEmpty) JsObjectIdentity.next else jsonId.get

          json ++ rightJson ++ Json.obj(JsObjectIdentity.name -> id)
        }
      }
    }

    saveOrUpdateRecords(linkedDsa.dataSetRepo, linkedJsons.toSeq, None, false, None, saveBatchSize)
  }

  // a helper function to load the jsons for a given data set and create a link -> jsons map
  private def linkJsonsMap(
    dataSetInfo: LinkedDataSetInfo,
    fieldTypeMap: Map[String, FieldType[_]],
    addDataSetIdToRightFieldNames: Boolean
  ): Future[Map[Seq[String], Traversable[JsObject]]] =
    for {
      jsons <- dataSetInfo.dsa.dataSetRepo.find(projection = dataSetInfo.fieldNamesToLoad)
    } yield {
      val linkFieldNameSet = dataSetInfo.linkFieldNames.toSet
      jsons.map { json =>
        val link = dataSetInfo.linkFieldNames.map { fieldName =>
          val fieldType = fieldTypeMap.get(fieldName).getOrElse(
            throw new AdaException(s"Field $fieldName not found.")
          )
          fieldType.jsonToDisplayString(json \ fieldName)
        }
        // remove the link fields from a json
        val strippedJson = json.fields.filterNot { case (fieldName, _) => linkFieldNameSet.contains(fieldName) }

        // rename if necessary
        if (addDataSetIdToRightFieldNames) {
          strippedJson.map { case (fieldName, jsValue) =>
            (dataSetInfo.dsa.dataSetId.replace('.', '_') + "-" + fieldName, jsValue)
          }
        }

        (link, JsObject(strippedJson))
      }.toGroupMap
    }


  def lalal(
    spec: MultiDataSetLinkSpec
  ) = {
    val leftDataSetInfo = dsaf(spec.leftSourceDataSetId).map { dsa =>
      LinkedDataSetInfo(dsa, spec.leftPreserveFieldNames, spec.leftLinkFieldNames)
    }.getOrElse(throw new AdaException(s"Data Set ${spec.leftSourceDataSetId} not found."))

    val leftDsa = leftDataSetInfo.dsa

    val rightDataSetInfos = spec.rightSourceDataSetIds.zip(spec.rightPreserveFieldNames).zip(spec.rightLinkFieldNames).map {
      case ((dataSetId, preserveFieldNames), linkFieldNames) =>
        val dsa = dsaf(dataSetId).getOrElse(throw new AdaException(s"Data Set ${dataSetId} not found."))
        LinkedDataSetInfo(dsa, preserveFieldNames, linkFieldNames)
    }

    val processingBatchSize = spec.processingBatchSize.getOrElse(10)
    val bufferSize = spec.backpressureBufferSize.getOrElse(10)

    // aux function that creates a data stream for a given dsa
    def createStream(info: LinkedDataSetInfo) = {
      if(info.fieldNamesToLoad.nonEmpty)
        logger.info(s"Creating a stream for these fields ${info.fieldNamesToLoad.mkString(",")}.")
      else
        logger.info(s"Creating a stream for all available fields.")

      leftDsa.dataSetRepo.findAsStream(
        sort = info.linkFieldNames.map(AscSort(_)),
        projection = info.fieldNamesToLoad
      )
    }

    // aux function to create orderings for the link fields
    def linkOrderings(
      info: LinkedDataSetInfo,
      fieldTypeMap: Map[String, FieldType[_]]
    ): Seq[Ordering[Any]] =
      info.linkFieldNames.map { fieldName =>
        val fieldType = fieldTypeMap.get(fieldName).get
        fieldTypeOrdering(fieldType.spec.fieldType).getOrElse(
          throw new AdaException(s"No ordering available for the field type ${fieldType.spec.fieldType}.")
        )
      }

    // aux function to convert a given json to a sequence of link values
    def linkValues(
      info: LinkedDataSetInfo,
      fieldTypeMap: Map[String, FieldType[_]])(
      json: JsObject
    ): Seq[Option[Any]] =
      info.linkFieldNames.map { fieldName =>
        val fieldType = fieldTypeMap.get(fieldName).get
        json.toValue(fieldName, fieldType)
      }

    for {
      // register the result data set (if not registered already)
      linkedDsa <- registerDerivedDataSet(leftDataSetInfo.dsa, spec.resultDataSetSpec)

      // get all the left data set fields
      leftFieldTypeMap <- fieldTypeMap(leftDataSetInfo)

      // get all the right data set fields
      rightFieldTypeMaps <- Future.sequence(rightDataSetInfos.map(fieldTypeMap))

      // left link fields' orderings
      leftLinkOrderings = linkOrderings(leftDataSetInfo, leftFieldTypeMap)

      // right link fields' orderings
      rightLinkOrderings = rightDataSetInfos.zip(rightFieldTypeMaps).map((linkOrderings(_,_)).tupled)

      // left stream
      leftStream <- createStream(leftDataSetInfo)

      // right streams
      rightSources <- Future.sequence(rightDataSetInfos.map(createStream))

      _ <- {
        val rightIterators = rightSources.map(_.runWith(StreamConverters.asJavaStream[JsObject]).iterator())

        var currentRightJson: Option[JsObject] = None
        var currentRightKey: Seq[Any] = Nil

        def findEqualLinkValues(
          info: LinkedDataSetInfo,
          fieldTypeMap: Map[String, FieldType[_]],
          linkOrderings: Seq[Ordering[Any]],
          jsonIterator: Iterator[JsObject])(
          leftLink: Seq[Option[Any]]
        ) = {

          jsonIterator.takeWhile { rightJson =>
            val rightLink = linkValues(info, fieldTypeMap)(rightJson)

            val isEqual = (leftLink.zip(rightLink)).zip(linkOrderings).forall { case ((leftValue, rightValue), ordering: Ordering[Any]) =>
              ordering.equiv(leftValue, rightValue)
            }
            isEqual
          }
        }

        leftStream.map { leftJson =>

//          if (currentRightJson.isEmpty && rightIterator.hasNext) {
//            currentRightJson = Some(rightIterator.next())
//            currentRightKey = key(currentRightJson.get)
//          }

          val leftLink = linkValues(leftDataSetInfo, leftFieldTypeMap)(leftJson)



          leftJson
        }
        Future(())
      }
    } yield
      ()
  }

  override def selfLink(spec: SelfLinkSpec) = {
    val dsa = dsaf(spec.dataSetId).get

    // helper function to merge jsons by ids into a single one, and save it
    def mergeAndSaveAux[T](
      newDsa: DataSetAccessor,
      valueFieldType: FieldType[T])(
      ids: Seq[BSONObjectID]
    ): Future[Unit] =
      for {
        jsons <- dsa.dataSetRepo.find(Seq(JsObjectIdentity.name #-> ids))

        _ <- {
          val newJsonValues = jsons.flatMap { json =>
            val prefixLabel = json.toDisplayString(spec.valueFieldName, valueFieldType)
            val prefix = prefixLabel.toLowerCase.replaceAllLiterally(" ", "_")

            json.value.toSeq.filter(!_._1.equals(JsObjectIdentity.name)).map { case (fieldName, value) =>
              (prefix + "_" + fieldName, value)
            }
          }

          newDsa.dataSetRepo.save(JsObject(newJsonValues.toSeq))
        }
      } yield
        ()

    // the main part
    for {
      // load jsons with key fields and id
      items <- dsa.dataSetRepo.find(projection = spec.keyFieldNames ++ Seq(JsObjectIdentity.name))

      // register a new data set
      newDsa <- registerDerivedDataSet(dsa, spec.resultDataSetSpec)

      // retrieve a value field
      valueField <- dsa.fieldRepo.get(spec.valueFieldName)

      // get all the fields
      allFields <- dsa.fieldRepo.find()

      // create the type of a value field
      valueFieldType = ftf(valueField.get.fieldTypeSpec)

      // get all the value field prefixes
      fieldPrefixes <- {
        dsa.dataSetRepo.find(projection = Seq(valueField.get.name)).map { jsons =>
          jsons.map { json =>
            val prefixLabel = json.toDisplayString(spec.valueFieldName, valueFieldType)
            val prefix = prefixLabel.toLowerCase.replaceAllLiterally(" ", "_")

            (prefix, prefixLabel)
          }.toSet
        }
      }

      // update the new dictionary
      _ <- {
        val newFields = allFields.flatMap(field =>
          fieldPrefixes.map { case (prefix, prefixLabel) =>
            field.copy(name = prefix + "_" + field.name, label = Some(prefixLabel + " " + field.label.getOrElse("")))
          }
        )

        updateDictionaryFields(newDsa.fieldRepo, newFields, true, true)
      }


      // delete the new data set (if contains any data)
      _ <- newDsa.dataSetRepo.deleteAll

      // group jsons by key fields, merge and save them
      _ <- {
        val idGroups = items.map { json =>
          val key = spec.keyFieldNames.map { fieldName =>
            (json \ fieldName).toOption
          }
          val id = (json \ JsObjectIdentity.name).as[BSONObjectID]
          (key, id)
        }.toGroupMap.map(_._2.toSeq)

        seqFutures(idGroups.grouped(spec.processingBatchSize.getOrElse(10))) { idGroups =>
          Future.sequence(
            idGroups.map(mergeAndSaveAux(newDsa, valueFieldType))
          )
        }
      }
    } yield
      ()
  }

  override def processSeriesAndSaveDataSet(
    spec: DataSetSeriesProcessingSpec
  ): Future[Unit] = {
    val dsa = dsaf(spec.sourceDataSetId).getOrElse(
      throw new AdaException(s"Data set id ${spec.sourceDataSetId} not found."))

    val processingBatchSize = spec.processingBatchSize.getOrElse(20)
    val saveBatchSize = spec.saveBatchSize.getOrElse(5)
    val preserveFieldNameSet = spec.preserveFieldNames.toSet

    for {
      // register the result data set (if not registered already)
      newDsa <- registerDerivedDataSet(dsa, spec.resultDataSetSpec)

      // get all the fields
      fields <- dsa.fieldRepo.find()

      // update the dictionary
      _ <- {
        val preservedFields = fields.filter(field => preserveFieldNameSet.contains(field.name))

        val newFields = spec.seriesProcessingSpecs.map(spec =>
          Field(spec.toString.replace('.', '_'), None, FieldTypeId.Double, true)
        )

        val fieldNameAndTypes = (preservedFields ++ newFields).map(field => (field.name, field.fieldTypeSpec))
        updateDictionary(spec.resultDataSetId, fieldNameAndTypes, false, true)
      }

      // delete all from the old data set
      _ <- newDsa.dataSetRepo.deleteAll

      // get all the ids
      ids <- dsa.dataSetRepo.allIds

      // process and save jsons
      _ <- seqFutures(ids.toSeq.grouped(processingBatchSize).zipWithIndex) {

        case (ids, groupIndex) =>
          Future.sequence(
            ids.map(dsa.dataSetRepo.get)
          ).map(_.flatten).flatMap { jsons =>

            logger.info(s"Processing series ${groupIndex * processingBatchSize} to ${(jsons.size - 1) + (groupIndex * processingBatchSize)}")
            val newJsons = jsons.par.map(processSeries(spec.seriesProcessingSpecs, preserveFieldNameSet)).toList

            // save the processed data set jsons
            saveOrUpdateRecords(newDsa.dataSetRepo, newJsons, None, false, None, Some(saveBatchSize))
          }
      }
    } yield
      ()
  }

  override def transformSeriesAndSaveDataSet(
    spec: DataSetSeriesTransformationSpec
  ) = {
    val dsa = dsaf(spec.sourceDataSetId).getOrElse(
      throw new AdaException(s"Data set id ${spec.sourceDataSetId} not found."))

    val processingBatchSize = spec.processingBatchSize.getOrElse(20)
    val saveBatchSize = spec.saveBatchSize.getOrElse(5)
    val preserveFieldNameSet = spec.preserveFieldNames.toSet

    for {
    // register the result data set (if not registered already)
      newDsa <- registerDerivedDataSet(dsa, spec.resultDataSetSpec)

      // get all the fields
      fields <- dsa.fieldRepo.find()

      // update the dictionary
      _ <- {
        val preservedFields = fields.filter(field => preserveFieldNameSet.contains(field.name))

        val newFields = spec.seriesTransformationSpecs.map(spec =>
          Field(spec.toString.replace('.', '_'), None, FieldTypeId.Double, true)
        )

        val fieldNameAndTypes = (preservedFields ++ newFields).map(field => (field.name, field.fieldTypeSpec))
        updateDictionary(spec.resultDataSetId, fieldNameAndTypes, false, true)
      }

//      // delete all from the old data set
//      _ <- newDsa.dataSetRepo.deleteAll
      ids <- dsa.dataSetRepo.allIds

      // TODO: quick fix that should be removed

      ids <- deltaIds(dsa.dataSetRepo, newDsa.dataSetRepo, "recordId").map { ids =>
        logger.info(s"Obtained ${ids.size} ids for a series transformation.")
        ids
      }

      // transform and save jsons
      _ <- seqFutures(ids.toSeq.grouped(processingBatchSize).zipWithIndex) {

        case (ids, groupIndex) =>
          Future.sequence(
            ids.map(dsa.dataSetRepo.get)
          ).map(_.flatten).flatMap { jsons =>

            logger.info(s"Transforming series ${groupIndex * processingBatchSize} to ${(jsons.size - 1) + (groupIndex * processingBatchSize)}")

            for {
              // transform jsons
              newJsons <- Future.sequence(
                jsons.map(transformSeries(spec.seriesTransformationSpecs, preserveFieldNameSet))
              )

              // save the transformed data set jsons
              _ <- saveOrUpdateRecords(newDsa.dataSetRepo, newJsons, None, false, None, Some(saveBatchSize))
            } yield
              ()
          }
      }
    } yield
      ()
  }

  private def deltaIds(
    sourceDataSetRepo: JsonCrudRepo,
    targetDataSetRepo: JsonCrudRepo,
    keyField: String
  ): Future[Traversable[BSONObjectID]] =
    for {
      existingNewRecordIds <- targetDataSetRepo.find(
        projection = Seq(keyField)
      ).map(_.map(json =>
        (json \ keyField).as[String]
      ))

      oldRecordIdIds <- sourceDataSetRepo.find(
        projection = Seq(idName, keyField),
        sort = Seq(AscSort(idName))
      ).map(_.map(json =>
        ((json \ keyField).as[String], (json \ idName).as[BSONObjectID])
      ))
    } yield {
      val existingNewRecordIdSet = existingNewRecordIds.toSet
      val deltaIds = oldRecordIdIds.filterNot { case (recordId, _) => existingNewRecordIdSet.contains(recordId) }.map(_._2)
      deltaIds.toSeq.sortBy(_.stringify)
    }

  private def transformSeries(
    transformationSpecs: Seq[SeriesTransformationSpec],
    preserveFieldNames: Set[String])(
    json: JsObject
  ): Future[JsObject] = {
    def transform(spec: SeriesTransformationSpec) = {
      val series = JsonUtil.traverse(json, spec.fieldPath).map(_.as[Double])

      for {
        newSeries <- if (series.nonEmpty)
          IOSeriesUtil.scaleSeries(sparkApp.session)(series.map(Seq(_)), spec.transformType)
        else
          Future(series.map(Seq(_)))
      } yield {
        val jsonSeries = newSeries.map(_.head).map(value =>
          if (value == null)
            throw new AdaException(s"Found a null value at the path ${spec.fieldPath}. \nSeries: ${series.mkString(",")}")
          else
            try {
              JsNumber(value)
            } catch {
              case e: NumberFormatException => throw new AdaException(s"Found a non-numeric value ${value} at the path ${spec.fieldPath}. \nSeries: ${series.mkString(",")}")
            }
        )

        spec.toString.replace('.', '_') -> JsArray(jsonSeries)
      }
    }

    for {
      newValues <- Future.sequence(transformationSpecs.map(transform))
    } yield {
      val preservedValues: Seq[(String, JsValue)] =
        json.fields.filter { case (fieldName, jsValue) => preserveFieldNames.contains(fieldName) }

      JsObject(preservedValues ++ newValues)
    }
  }

  private def processSeries(
    processingSpecs: Seq[SeriesProcessingSpec],
    preserveFieldNames: Set[String])(
    json: JsObject
  ): JsObject = {
    val newValues = processingSpecs.par.map { spec =>
      val series = JsonUtil.traverse(json, spec.fieldPath).map(_.as[Double])
      val newSeries: Seq[Double] = processSeries(series, spec)

      spec.toString.replace('.','_') -> JsArray(newSeries.map( value =>
        if (value == null)
          throw new AdaException(s"Found a null value at the path ${spec.fieldPath}. \nSeries: ${series.mkString(",")}")
        else
          try {
            JsNumber(value)
          } catch {
            case e: NumberFormatException => throw new AdaException(s"Found a non-numeric value ${value} at the path ${spec.fieldPath}. \nSeries: ${series.mkString(",")}")
          }
      ))
    }.toList

    val preservedValues: Seq[(String, JsValue)] =
      json.fields.filter { case (fieldName, jsValue) => preserveFieldNames.contains(fieldName)}

    JsObject(preservedValues ++ newValues)
  }

  private def processSeries(
    series: Seq[Double],
    spec: SeriesProcessingSpec
  ): Seq[Double] = {
    val newSeries = series.sliding(spec.pastValuesCount + 1).map { values =>

      def process(seq: Seq[Double]): Seq[Double] = {
        if (seq.size == 1)
          seq
        else {
          val newSeq = spec.processingType match {
            case SeriesProcessingType.Diff => values.zip(values.tail).map { case (a, b) => b - a }
            case SeriesProcessingType.RelativeDiff => values.zip(values.tail).map { case (a, b) => (b - a) / a }
            case SeriesProcessingType.Ratio => values.zip(values.tail).map { case (a, b) => b / a }
            case SeriesProcessingType.LogRatio => values.zip(values.tail).map { case (a, b) => Math.log(b / a) }
            case SeriesProcessingType.Min => Seq(values.min)
            case SeriesProcessingType.Max => Seq(values.max)
            case SeriesProcessingType.Mean => Seq(values.sum / values.size)
          }
          process(newSeq)
        }
      }

      process(values).head
    }.toSeq

    // add an initial padding if needed
    if (spec.addInitPaddingWithZeroes)
      Seq.fill(spec.pastValuesCount)(0d) ++ newSeries
    else
      newSeries
  }

  override def extractPeaks(
    series: Seq[Double],
    peakNum: Int,
    peakSelectionRatio: Option[Double]
  ): Option[Seq[Double]] = {
    val (minIndeces, maxIndeces) = localMinMaxIndeces(series)
//    val maxIndecesWithValues = maxIndeces.zip(maxIndeces.map(series(_)))

    val selectedMinIndeces =
      peakSelectionRatio.map { ratio =>
        val minIndecesWithValues = minIndeces.zip(minIndeces.map(series(_)))
        val topMins = minIndecesWithValues.sortBy(_._2).take((minIndeces.size * ratio).floor.toInt)
        topMins.map(_._1).sorted
      }.getOrElse(
        minIndeces
      )

    if (selectedMinIndeces.size > peakNum)
      Some(series.take(selectedMinIndeces(peakNum)).drop(selectedMinIndeces(0)))
    else
      None
  }

  private def localMinMaxIndeces(
    series: Seq[Double]
  ): (Seq[Int], Seq[Int]) = {
    val diffs = series.zip(series.tail).map { case (a, b) => b - a}
    val maxima = diffs.sliding(2).zipWithIndex.filter { case (diff, index) =>
      diff(0) > 0 && diff(1) <= 0
    }
    val minima = diffs.sliding(2).zipWithIndex.filter { case (diff, index) =>
      diff(0) < 0 && diff(1) >= 0
    }
    (minima.map(_._2 + 1).toSeq, maxima.map(_._2 + 1).toSeq)
  }

  private def registerDerivedDataSet(
    sourceDsa: DataSetAccessor,
    spec: ResultDataSetSpec
  ): Future[DataSetAccessor] = {
    val metaInfoFuture = sourceDsa.metaInfo
    val settingFuture = sourceDsa.setting

    for {
      // get the data set meta info
      metaInfo <- metaInfoFuture

      // get the data set setting
      setting <- settingFuture

      // register the data set (if not registered already)
      newDsa <- dsaf.register(
        metaInfo.copy(_id = None, id = spec.id, name = spec.name, timeCreated = new ju.Date()),
        Some(setting.copy(_id = None, dataSetId = spec.id, storageType = spec.storageType)),
        None
      )
    } yield
      newDsa
  }

  override def register(
    sourceDsa: DataSetAccessor,
    newDataSetId: String,
    newDataSetName: String,
    newStorageType: StorageType.Value
  ): Future[DataSetAccessor] = {
    for {
      // get the data set meta info
      metaInfo <- sourceDsa.metaInfo

      // register the norm data set (if not registered already)
      newDsa <- dsaf.register(
        metaInfo.copy(_id = None, id = newDataSetId, name = newDataSetName, timeCreated = new ju.Date()),
        Some(new DataSetSetting(newDataSetId, newStorageType)),
        None
      )
    } yield
      newDsa
  }

  private def inferFieldTypes(
    jsonFieldTypeInferrer: FieldTypeInferrer[JsReadable],
    fieldNames: Traversable[String])(
    items: Traversable[JsObject]
  ): Traversable[(String, FieldType[_])] =
    fieldNames.map { fieldName =>
      val jsons = project(items, fieldName)
      (fieldName, jsonFieldTypeInferrer(jsons))
    }

  override def updateDictionary(
    dataSetId: String,
    fieldNameAndTypes: Traversable[(String, FieldTypeSpec)],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit] = {
    logger.info(s"Dictionary update for data set '${dataSetId}' initiated.")

    val dsa = dsaf(dataSetId).get
    val fieldRepo = dsa.fieldRepo

    updateDictionary(fieldRepo, fieldNameAndTypes, deleteAndSave, deleteNonReferenced).map(_ =>
      messageLogger.info(s"Dictionary update for '${dataSetId}' successfully finished.")
    )
  }

  override def updateDictionaryFields(
    dataSetId: String,
    newFields: Traversable[Field],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit] = {
    logger.info(s"Dictionary update for data set '${dataSetId}' initiated.")

    val dsa = dsaf(dataSetId).get
    val fieldRepo = dsa.fieldRepo

    updateDictionaryFields(fieldRepo, newFields, deleteAndSave, deleteNonReferenced).map(_ =>
      messageLogger.info(s"Dictionary update for '${dataSetId}' successfully finished.")
    )
  }

  override def updateDictionary(
    fieldRepo: FieldRepo,
    fieldNameAndTypes: Traversable[(String, FieldTypeSpec)],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit] = {
    val newFields = fieldNameAndTypes.map { case (fieldName, fieldType) =>
      val stringEnums = fieldType.enumValues.map { case (from, to) => (from.toString, to) }
      Field(fieldName, None, fieldType.fieldType, fieldType.isArray, stringEnums, fieldType.displayDecimalPlaces)
    }
    updateDictionaryFields(fieldRepo, newFields, deleteAndSave, deleteNonReferenced)
  }

  override def updateDictionaryFields(
    fieldRepo: FieldRepo,
    newFields: Traversable[Field],
    deleteAndSave: Boolean,
    deleteNonReferenced: Boolean
  ): Future[Unit] = {
    val newFieldNames = newFields.map(_.name).toSeq

    for {
      // get the existing fields
      referencedFields <-
        fieldRepo.find(Seq(FieldIdentity.name #-> newFieldNames))

      referencedNameFieldMap = referencedFields.map(field => (field.name, field)).toMap

      // get the non-existing fields
      nonReferencedFields <-
        fieldRepo.find(Seq(FieldIdentity.name #!-> newFieldNames))

      // fields to save or update
      fieldsToSaveAndUpdate: Traversable[Either[Field, Field]] =
        newFields.map { newField =>
          referencedNameFieldMap.get(newField.name) match {

            case None => Left(newField)

            case Some(oldField) => Right(
              oldField.copy(
                fieldType = newField.fieldType,
                isArray = newField.isArray,
                enumValues = newField.enumValues,
                label = oldField.label match {
                  case Some(label) => Some(label)
                  case None => newField.label
                }
              )
            )
          }
        }

      // fields to save
      fieldsToSave = fieldsToSaveAndUpdate.map(_.left.toOption).flatten

      // save the new fields
      _ <- fieldRepo.save(fieldsToSave)

      // fields to update
      fieldsToUpdate = fieldsToSaveAndUpdate.map(_.right.toOption).flatten

      // update the existing fields
      _ <- if (deleteAndSave)
        fieldRepo.delete(fieldsToUpdate.map(_.name)).flatMap { _ =>
          fieldRepo.save(fieldsToUpdate)
        }
      else
        fieldRepo.update(fieldsToUpdate)

      // remove the non-referenced fields if needed
      _ <- if (deleteNonReferenced)
        fieldRepo.delete(nonReferencedFields.map(_.name))
      else
        Future(())

    } yield
      ()
  }

  override def translateDataAndDictionary(
    originalDataSetId: String,
    newDataSetId: String,
    newDataSetName: String,
    newDataSetSetting: Option[DataSetSetting],
    newDataView: Option[DataView],
    useTranslations: Boolean,
    removeNullColumns: Boolean,
    removeNullRows: Boolean
  ) = {
    logger.info(s"Translation of the data and dictionary for data set '${originalDataSetId}' initiated.")
    val originalDsa = dsaf(originalDataSetId).get
    val originalDataRepo = originalDsa.dataSetRepo
    val originalDictionaryRepo = originalDsa.fieldRepo

    for {
      // get the accessor (data repo and field repo) for the newly registered data set
      originalDataSetInfo <- originalDsa.metaInfo
      newDsa <- dsaf.register(
        DataSetMetaInfo(None, newDataSetId, newDataSetName, 0, false, originalDataSetInfo.dataSpaceId), newDataSetSetting, newDataView
      )
      newFieldRepo = newDsa.fieldRepo

      // obtain the translation map
      translationMap <- if (useTranslations) {
          translationRepo.find().map(
            _.map(translation => (translation.original, translation.translated)).toMap
          )
        } else {
          Future(Map[String, String]())
        }

      // get the original dictionary fields
      originalFields <- originalDictionaryRepo.find()

      // get the field types
      originalFieldNameAndTypes = originalFields.map(field => (field.name, field.fieldTypeSpec)).toSeq

      // get the items (from the data set)
      items <- originalDataRepo.find(sort = Seq(AscSort(dataSetIdFieldName)))

      // transform the items and dictionary
      (newJsons, newFieldNameAndTypes) = translateDataAndDictionary(
        items, originalFieldNameAndTypes, translationMap, true, true)

      // update the dictionary
      _ <- updateDictionary(newFieldRepo, newFieldNameAndTypes, false, true)

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- newDsa.updateDataSetRepo

      // get the new data set repo
      newDataRepo = newDsa.dataSetRepo

      // delete all the data
      _ <- {
        logger.info(s"Deleting all the data for '${newDataSetId}'.")
        newDataRepo.deleteAll
      }

      // save the new items
      _ <- saveOrUpdateRecords(newDataRepo, newJsons.toSeq)
    } yield
      messageLogger.info(s"Translation of the data and dictionary for data set '${originalDataSetId}' successfully finished.")
  }

  private def createNewJsonsAndSave(
    originalDataRepo: JsonCrudRepo,
    newDataRepo: JsonCrudRepo,
    newFieldNameAndTypeMap: Map[String, FieldType[_]],
    overallSize: Int,
    batchSize: Int)(
    idsIndex: (Seq[BSONObjectID], Int)
  ): Future[Traversable[BSONObjectID]] = {
    // helper functions to parse jsons
    def displayJsonToJson[T](fieldType: FieldType[T], json: JsReadable): JsValue = {
      val value = fieldType.displayJsonToValue(json)
      fieldType.valueToJson(value)
    }

    val ids = idsIndex._1
    val index = idsIndex._2
    val reportLineSize = overallSize * reportLineFreq

    val newJsonsFuture: Future[Traversable[JsObject]] =
      originalDataRepo.find(Seq(JsObjectIdentity.name #-> ids)).map { originalItems =>
        originalItems.map { originalItem =>
          val newJsonValues = originalItem.fields.map { case (fieldName, jsonValue) =>
            val newJsonValue = newFieldNameAndTypeMap.get(fieldName) match {
              case Some(newFieldType) => displayJsonToJson(newFieldType, jsonValue)
              case None => jsonValue
            }
            (fieldName, newJsonValue)
          }
          JsObject(newJsonValues)
        }
      }
    //            ids.map { id =>
    //              originalDataRepo.get(id).map { case Some(originalItem) =>
    //
    //                val newJsonValues = originalItem.fields.map { case (fieldName, jsonValue) =>
    //                  val newJsonValue = newFieldNameAndTypeMap.get(fieldName) match {
    //                    case Some(newFieldType) => displayJsonToJson(newFieldType, jsonValue)
    //                    case None => jsonValue
    //                  }
    //                  (fieldName, newJsonValue)
    //                }
    //
    //                JsObject(newJsonValues)
    //              }
    //            }

    newJsonsFuture.flatMap { newJsons =>
      newDataRepo.save(newJsons).map { ids =>
        try {
          logProgress((index + 1) * batchSize, reportLineSize, overallSize)
          ids
        } catch {
          case e: Exception => throw e
        }
      }
    }
  }

  override def copyToNewStorage(
    dataSetId: String,
    groupSize: Int,
    parallelism: Int,
    backpressureBufferSize: Int,
    saveDeltaOnly: Boolean,
    targetStorageType: StorageType.Value
  ): Future[Unit] = {
    val dsa = dsaf(dataSetId: String).get
    val originalDataSetRepo = dsa.dataSetRepo
    for {
      // setting
      setting <- dsa.setting

      // stream
      stream <- originalDataSetRepo.findAsStream()

      // switch the storage type
      _ <- dsa.updateDataSetRepo(setting.copy(storageType = targetStorageType))

      // new data set repo
      newDataSetRepo = dsa.dataSetRepo

      // delete the new data set (at a new storage) if needed
      _ <- if (!saveDeltaOnly) newDataSetRepo.deleteAll else Future(())

      // existing ids at the new data set
      existingIds <- Future(Nil) // newDataSetRepo.asInstanceOf[JsonReadonlyRepo].allIds.map(_.toSet)

      // group and save the stream as it goes
      _ <- stream.map{ json => logger.info("Reading from the source DB..."); json}
        .grouped(groupSize)
        .buffer(backpressureBufferSize, OverflowStrategy.backpressure)
        .mapAsync(parallelism){ jsons =>
          val newJsons = jsons.filterNot { json =>
            val id  = (json \ idName).as[BSONObjectID]
            existingIds.contains(id)
          }
          if (newJsons.size != jsons.size)
            logger.info(s"Skipping ${jsons.size - newJsons.size} items.")
          logger.info(s"Saving ${newJsons.size} items.")
          newDataSetRepo.save(newJsons)
        }.runWith(Sink.ignore)
    } yield
      ()
  }

//  def standardizeAllNumericFields(
//    sourceDataSetId: String,
//    resultDataSetSpec: ResultDataSetSpec,
//    streamSpec: StreamSpec
//  ): Future[Unit] = {
//    val sourceDsa = dsaf(sourceDataSetId).get
//
//    for {
//      // get all the numeric fields
//      numericFields <- sourceDsa.fieldRepo.find(Seq("fieldType" #-> Seq(FieldTypeId.Integer, FieldTypeId.Double, FieldTypeId.Date)))
//
//      // input data stream
//      inputStream <- sourceDsa.dataSetRepo.findAsStream(projection = numericFields.map(_.name))
//
////      _ <- saveDerivedDataSet(sourceDsa, spec.resultDataSetSpec, inputStream, fieldsToKeep.toSeq, spec.streamSpec, true)
//    } yield
//      ()
//  }

  override def saveDerivedDataSet(
    sourceDsa: DataSetAccessor,
    derivedDataSetSpec: ResultDataSetSpec,
    inputSource: Source[JsObject, _],
    fields: Traversable[Field],
    streamSpec: StreamSpec = StreamSpec(),
    saveViewsAndFiltersFlag: Boolean = true
  ): Future[Unit] = {
    for {
      // register a new data set
      targetDsa <- registerDerivedDataSet(sourceDsa, derivedDataSetSpec)

      // reference category ids from the original data set
      refCategoryIds = fields.flatMap(_.categoryId).toSet.toSeq

      // delete all the new categories (if any)
      _ <- targetDsa.categoryRepo.deleteAll

      // save the referenced categories and collect new ids
      newCategoryIds <- sourceDsa.categoryRepo.find(Seq(CategoryIdentity.name #-> refCategoryIds.map(Some(_)))).flatMap(categories =>
        targetDsa.categoryRepo.save(categories.map(_.copy(_id = None)))
      )

      // old -> new category id map
      oldNewCategoryIdMap = refCategoryIds.zip(newCategoryIds.toSeq).toMap

      // new fields (with replaced category ids)
      newFields = fields.map(field => field.copy(categoryId = field.categoryId.flatMap(oldNewCategoryIdMap.get)))

      // delete all the new fields (if any)
      _ <- targetDsa.fieldRepo.deleteAll

      _ <- targetDsa.fieldRepo.flushOps

      // save the new fields (minus the dropped ones)
      _ <- targetDsa.fieldRepo.save(newFields)

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- targetDsa.updateDataSetRepo

      // delete the new data set if needed
      _ <- targetDsa.dataSetRepo.deleteAll

      // group and save the stream as it goes
      _ <- {
        logger.info(s"Streaming data from ${sourceDsa.dataSetId} to ${derivedDataSetSpec.id}...")
        targetDsa.dataSetRepo.saveAsStream(inputSource, streamSpec)
      }

      // handle the filters and views (if needed)
      _ <- if (saveViewsAndFiltersFlag) saveViewsAndFilters(sourceDsa, targetDsa, fields) else Future(())
    } yield
      ()
  }

  private def saveViewsAndFilters(
    sourceDsa: DataSetAccessor,
    targetDsa: DataSetAccessor,
    fields: Traversable[Field]
  ): Future[Unit] =
    for {
      // get the original filters
      oldFilters <- sourceDsa.filterRepo.find()

      // field name set for a quick lookup
      fieldNameSet = fields.map(_.name).toSet

      // collect the filters with the available fields
      refFilters = oldFilters.filter(filter => filter.conditions.map(_.fieldName).forall(fieldNameSet.contains))

      // delete all the new filters (if any)
      _ <- targetDsa.filterRepo.deleteAll

      // save the new filters
      newFilterIds <- targetDsa.filterRepo.save(refFilters.map(_.copy(_id = None, timeCreated = Some(new java.util.Date()))))

      // old -> new filter id map
      oldNewFilterIdMap = refFilters.toSeq.map(_._id.get).zip(newFilterIds.toSeq).toMap

      // get the original views
      oldViews <- sourceDsa.dataViewRepo.find()

      // referenced filter id set for a quick lookup
      refFilterIdSet = refFilters.flatMap(_._id).toSet

      // collect the views for the available filters (and fields)
      refViews = oldViews.filter { view =>
        val filterIds = view.filterOrIds.flatMap(_.right.toOption)
        val conditionFieldIds = view.filterOrIds.flatMap(_.left.toOption.map(_.map(_.fieldName))).flatten
        filterIds.forall(refFilterIdSet.contains) && conditionFieldIds.forall(fieldNameSet.contains)
      }

      // create new views
      newViews = refViews.map { view =>

        val newFilterOrIds = view.filterOrIds.map(
          _ match {
            case Left(conditions) => Left(conditions)
            case Right(filterId) => Right(oldNewFilterIdMap.get(filterId).get)
          }
        )

        val newWidgets = view.widgetSpecs.filter(widgetSpec =>
          widgetSpec.fieldNames.forall(fieldNameSet.contains)
        )

        val newTableColumnNames = view.tableColumnNames.filter(fieldNameSet.contains)

        view.copy(_id = None, timeCreated = new java.util.Date(), filterOrIds = newFilterOrIds, widgetSpecs = newWidgets, tableColumnNames = newTableColumnNames)
      }

      // delete all the new views (if any)
      _ <- targetDsa.dataViewRepo.deleteAll

      // save the new views
      _ <- targetDsa.dataViewRepo.save(newViews)
    } yield
      ()

  override def translateDataAndDictionaryOptimal(
    originalDataSetId: String,
    newDataSetId: String,
    newDataSetName: String,
    newDataSetSetting: Option[DataSetSetting],
    newDataView: Option[DataView],
    saveBatchSize: Option[Int],
    inferenceGroupSize: Option[Int],
    inferenceGroupsInParallel: Option[Int],
    jsonFieldTypeInferrer: Option[FieldTypeInferrer[JsReadable]]
  ) = {
    logger.info(s"Translation of the data and dictionary for data set '${originalDataSetId}' initiated.")
    val originalDsa = dsaf(originalDataSetId).get
    val originalDataRepo = originalDsa.dataSetRepo
    val originalDictionaryRepo = originalDsa.fieldRepo
    val originalCategoryRepo = originalDsa.categoryRepo

    val inferenceGroupsInParallelInit = inferenceGroupsInParallel.getOrElse(2)
    val inferenceGroupSizeInit = inferenceGroupSize.getOrElse(10)

    for {
      // get the accessor (data repo and field repo) for the newly registered data set
      originalDataSetInfo <- originalDsa.metaInfo
      newDsa <- dsaf.register(
        DataSetMetaInfo(None, newDataSetId, newDataSetName, 0, false, originalDataSetInfo.dataSpaceId),
        newDataSetSetting,
        newDataView
      )
      newFieldRepo = newDsa.fieldRepo
      newCategoryRepo = newDsa.categoryRepo

      // get the original dictionary fields
      originalFields <- originalDictionaryRepo.find()

      // get the original categories
      originalCategories <- originalCategoryRepo.find()

      // get the field types
      originalFieldNameAndTypes = originalFields.toSeq.map(field => (field.name, field.fieldTypeSpec))
      originalFieldNameMap = originalFields.map(field => (field.name, field)).toMap

      newFieldNameAndTypes <- {
        logger.info("Inferring new field types started")
        val fieldNames = originalFieldNameAndTypes.map(_._1).sorted

        seqFutures(fieldNames.grouped(inferenceGroupsInParallelInit * inferenceGroupSizeInit)) { fieldsNames =>
          logger.info(s"Inferring new field types for ${fieldsNames.size} fields")
          inferFieldTypesInParallel(originalDataRepo, fieldsNames, inferenceGroupSizeInit, jsonFieldTypeInferrer)
        }.map(_.flatten)
      }

      // update the dictionary
//      _ <- updateDictionary(newFieldRepo, newFieldNameAndTypes.map { case (fieldName, fieldType) => (fieldName, fieldType.spec)}, false, true)

      // delete all the new fields
      _ <- newFieldRepo.deleteAll

      // save the new fields
      _ <- {
        val newFields = newFieldNameAndTypes.map { case (fieldName, fieldType) =>
          val fieldTypeSpec = fieldType.spec
          val stringEnums = fieldTypeSpec.enumValues.map { case (from, to) => (from.toString, to)}

          originalFieldNameMap.get(fieldName).map( field =>
            field.copy(fieldType = fieldTypeSpec.fieldType, isArray = fieldTypeSpec.isArray, enumValues = stringEnums)
          )
        }.flatten
        newFieldRepo.save(newFields)
      }

      // delete all the new categories
      _ <- newCategoryRepo.deleteAll

      // save all the new categories
      _ <- newCategoryRepo.save(originalCategories)

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- newDsa.updateDataSetRepo

      // get the new data set repo
      newDataRepo = newDsa.dataSetRepo

      // delete all the data
      _ <- {
        logger.info(s"Deleting all the data for '${newDataSetId}'.")
        newDataRepo.deleteAll
      }

      originalIds <- {
        logger.info("Getting the original ids")
        // get the items (from the data set)
        originalDataRepo.find(
          projection = Seq(dataSetIdFieldName),
          sort = Seq(AscSort(dataSetIdFieldName))
        ).map(_.map(json => (json \ dataSetIdFieldName).as[BSONObjectID]))
      }

      // save the new items
      _ <- {
        logger.info("Saving new items")
        val newFieldNameAndTypeMap: Map[String, FieldType[_]] = newFieldNameAndTypes.toMap
        val size = originalIds.size
        seqFutures(
          originalIds.toSeq.grouped(saveBatchSize.getOrElse(1)).zipWithIndex
        )(
          createNewJsonsAndSave(originalDataRepo, newDataRepo, newFieldNameAndTypeMap, size, saveBatchSize.getOrElse(1))
        )
      }
    } yield
      messageLogger.info(s"Translation of the data and dictionary for data set '${originalDataSetId}' successfully finished.")
  }

  override def translateData(
    originalDataSetId: String,
    newDataSetId: String,
    newDataSetName: String,
    newDataSetSetting: Option[DataSetSetting],
    newDataView: Option[DataView],
    saveBatchSize: Option[Int]
  ) = {
    logger.info(s"Translation of the data using a given dictionary for data set '${originalDataSetId}' initiated.")
    val originalDsa = dsaf(originalDataSetId).get
    val originalDataRepo = originalDsa.dataSetRepo

    for {
    // get the accessor (data repo and field repo) for the newly registered data set
      originalDataSetInfo <- originalDsa.metaInfo
      newDsa <- dsaf.register(
        DataSetMetaInfo(None, newDataSetId, newDataSetName, 0, false, originalDataSetInfo.dataSpaceId),
        newDataSetSetting,
        newDataView
      )

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- newDsa.updateDataSetRepo

      // get the new data set repo
      newDataRepo = newDsa.dataSetRepo

      // get all the fields
      newFields <- newDsa.fieldRepo.find()

      // delete all the data
      _ <- {
        logger.info(s"Deleting all the data for '${newDataSetId}'.")
        newDataRepo.deleteAll
      }

      originalIds <- {
        logger.info("Getting the original ids")
        // get the items (from the data set)
        originalDataRepo.find(
          projection = Seq(dataSetIdFieldName),
          sort = Seq(AscSort(dataSetIdFieldName))
        ).map(_.map(json => (json \ dataSetIdFieldName).as[BSONObjectID]))
      }

      // save the new items
      _ <- {
        logger.info("Saving new items")
        val newFieldNameAndTypeMap: Map[String, FieldType[_]] = newFields.map(field => (field.name, ftf(field.fieldTypeSpec))).toMap
        val size = originalIds.size
        seqFutures(
          originalIds.toSeq.grouped(saveBatchSize.getOrElse(1)).zipWithIndex
        ) {
          createNewJsonsAndSave(originalDataRepo, newDataRepo, newFieldNameAndTypeMap, size, saveBatchSize.getOrElse(1))
        }
      }
    } yield
      messageLogger.info(s"Translation of the data using a given dictionary for data set '${originalDataSetId}' successfully finished.")
  }

  protected def translateDataAndDictionary(
    items: Traversable[JsObject],
    fieldNameAndTypes: Seq[(String, FieldTypeSpec)],
    translationMap: Map[String, String],
    removeNullColumns: Boolean,
    removeNullRows: Boolean
  ): (Traversable[JsObject], Seq[(String, FieldTypeSpec)]) = {
    val nullFieldNameSet = fieldNameAndTypes.filter(_._2.fieldType == FieldTypeId.Null).map(_._1).toSet

    // get the string or enum scalar field types
    // TODO: what about arrays?
    val stringOrEnumScalarFieldTypes = fieldNameAndTypes.filter { fieldNameAndType =>
      val fieldTypeSpec = fieldNameAndType._2
      (!fieldTypeSpec.isArray && (fieldTypeSpec.fieldType == FieldTypeId.String || fieldTypeSpec.fieldType == FieldTypeId.Enum))
    }

    val stringFieldNames = stringOrEnumScalarFieldTypes.filter(_._2.fieldType == FieldTypeId.String).map(_._1)
    val enumFieldNames = stringOrEnumScalarFieldTypes.filter(_._2.fieldType == FieldTypeId.Enum).map(_._1)

    // translate strings and enums
    val (convertedJsons, newFieldTypes) = translateFields(items, stringOrEnumScalarFieldTypes, translationMap)

    val convertedJsItems = (items, convertedJsons).zipped.map {
      case (json, convertedJson: JsObject) =>
        // remove null columns
        val nonNullValues =
          if (removeNullColumns) {
            json.fields.filter { case (fieldName, _) => !nullFieldNameSet.contains(fieldName) && !fieldName.equals(dataSetIdFieldName) }
          } else
            json.fields.filter { case (fieldName, _) => !fieldName.equals(dataSetIdFieldName)}

        // merge with String-converted Jsons
        JsObject(nonNullValues) ++ convertedJson
    }

    // remove all items without any content
    val finalJsons = if (removeNullRows) {
      convertedJsItems.filter(item =>
        item.fields.exists { case (fieldName, value) =>
          fieldName != dataSetIdFieldName && value != JsNull
        }
      )
    } else
      convertedJsItems

    // remove null columns if needed
    val nonNullFieldNameAndTypes =
      if (removeNullColumns) {
        fieldNameAndTypes.filter { case (fieldName, _) => !nullFieldNameSet.contains(fieldName)}
      } else
        fieldNameAndTypes

    // merge string and enum field name type maps
    val newFieldNameTypeMap = (stringOrEnumScalarFieldTypes.map(_._1), newFieldTypes).zipped.toMap

    def countFromOld(fieldTypeId: FieldTypeId.Value) =
      newFieldNameTypeMap.count { case (name, fieldTypeSpec) => fieldTypeSpec.fieldType == fieldTypeId}

    def countFromOldEnum(fieldTypeId: FieldTypeId.Value) =
      newFieldNameTypeMap.count { case (name, fieldTypeSpec) =>
        enumFieldNames.contains(name) && fieldTypeSpec.fieldType == fieldTypeId
      }

    def countFromOldString(fieldTypeId: FieldTypeId.Value) =
      newFieldNameTypeMap.count { case (name, fieldTypeSpec) =>
        stringFieldNames.contains(name) && fieldTypeSpec.fieldType == fieldTypeId
      }


    println("Old strings and enums : " + stringOrEnumScalarFieldTypes.size)
    println("--------------------------")
    println("Strings   : " + stringFieldNames.size)
    println("Enums     : " + enumFieldNames.size)
    println()
    println("Total new types: " + newFieldTypes.size)
    println("-----------------")
    println("Strings   : " + countFromOld(FieldTypeId.String))
    println("Enums     : " + countFromOld(FieldTypeId.Enum))
    println("Booleans  : " + countFromOld(FieldTypeId.Boolean))
    println("Integers  : " + countFromOld(FieldTypeId.Integer))
    println("Doubles   : " + countFromOld(FieldTypeId.Double))
    println("Nulls     : " + countFromOld(FieldTypeId.Null))
    println("Dates     : " + countFromOld(FieldTypeId.Date))
    println()
    println("String -> ...")
    println("-------------")
    println("Strings   : " + countFromOldString(FieldTypeId.String))
    println("Enums     : " + countFromOldString(FieldTypeId.Enum))
    println("Booleans  : " + countFromOldString(FieldTypeId.Boolean))
    println("Integers  : " + countFromOldString(FieldTypeId.Integer))
    println("Doubles   : " + countFromOldString(FieldTypeId.Double))
    println("Nulls     : " + countFromOldString(FieldTypeId.Null))
    println("Dates     : " + countFromOldString(FieldTypeId.Date))
    println()
    println("Enum -> ...")
    println("-----------")
    println("Strings   : " + countFromOldEnum(FieldTypeId.String))
    println("Enums     : " + countFromOldEnum(FieldTypeId.Enum))
    println("Booleans  : " + countFromOldEnum(FieldTypeId.Boolean))
    println("Integers  : " + countFromOldEnum(FieldTypeId.Integer))
    println("Doubles   : " + countFromOldEnum(FieldTypeId.Double))
    println("Nulls     : " + countFromOldEnum(FieldTypeId.Null))
    println("Dates     : " + countFromOldEnum(FieldTypeId.Date))
    println()

    // update the field types with the new ones
    val finalFieldNameAndTypes = nonNullFieldNameAndTypes.map { case (fieldName, fieldType) =>
      val newFieldType = newFieldNameTypeMap.get(fieldName).getOrElse(fieldType)
      (fieldName, newFieldType)
    }

    (finalJsons, finalFieldNameAndTypes)
  }

  protected def createJsonsWithFieldTypes(
    fieldNames: Seq[String],
    values: Seq[Seq[String]],
    fieldTypeInferrer: FieldTypeInferrer[String]
  ): (Seq[JsObject], Seq[(String, FieldType[_])]) = {
    val fieldTypes = values.transpose.par.map(fieldTypeInferrer.apply).toList

    val jsons = values.map( vals =>
      JsObject(
        (fieldNames, fieldTypes, vals).zipped.map {
          case (fieldName, fieldType, text) =>
            val jsonValue = fieldType.displayStringToJson(text)
            (fieldName, jsonValue)
        })
    )

    (jsons, fieldNames.zip(fieldTypes))
  }

  private def translateFields(
    items: Traversable[JsObject],
    fieldNameAndTypeSpecs: Seq[(String, FieldTypeSpec)],
    translationMap: Map[String, String]
  ): (Seq[JsObject], Seq[FieldTypeSpec]) = {

    // obtain field types from the specs
    val fieldNameAndTypes = fieldNameAndTypeSpecs.map { case (fieldName, fieldTypeSpec) =>
      (fieldName, ftf(fieldTypeSpec))
    }

    // translate jsons to String values
    def translate(json: JsObject) = fieldNameAndTypes.map { case (fieldName, fieldType) =>
      val stringValue = fieldType.jsonToDisplayString(json \ fieldName)
      translationMap.get(stringValue).getOrElse(stringValue)
    }

    val convertedStringValues = items.map(translate)

    val noCommaFtf = FieldTypeHelper.fieldTypeFactory(arrayDelimiter = ",,,", booleanIncludeNumbers = false)
    val noCommanFti = FieldTypeHelper.fieldTypeInferrerFactory(ftf = noCommaFtf, arrayDelimiter = ",,,").apply

    // infer new types
    val (jsons, fieldTypes) = createJsonsWithFieldTypes(
      fieldNameAndTypeSpecs.map(_._1),
      convertedStringValues.toSeq,
      noCommanFti
    )

    (jsons, fieldTypes.map(_._2.spec))
  }

  override def getColumnNameLabels(
    delimiter: String,
    lineIterator: Iterator[String]
  ): Seq[(String, String)] =
    lineIterator.take(1).flatMap {
      _.split(delimiter).map { columnName =>
        val trimmedName = columnName.trim.replaceAll("\"", "")
        (nonAlphanumericToUnderscore(trimmedName), trimmedName)
      }
    }.toList

  // parse the line, returns the parsed items
  override def parseLine(
    delimiter: String,
    line: String,
    prefixSuffixSeparators: Seq[(String, String)] = Nil
  ): Seq[String] = {
    val itemsWithPrefixAndSuffix = line.split(delimiter, -1).map { l =>
      val trimmed = l.trim

      if (prefixSuffixSeparators.nonEmpty) {
        val (item, prefixSuffix) = handlePrefixSuffixes(trimmed, prefixSuffixSeparators)

        // TODO: this seems very ad-hoc and should be investigated where it is actually used
        val newItem = item.replaceAll("\\\\\"", "\"")

        (newItem, prefixSuffix)
      } else {
        (trimmed, None)
      }
    }

    fixImproperPrefixSuffix(delimiter, itemsWithPrefixAndSuffix)
  }

  private def handlePrefixSuffixes(
    string: String,
    prefixSuffixStrings: Seq[(String, String)]
  ): (String, Option[PrefixSuffix]) =
    prefixSuffixStrings.foldLeft((string, Option.empty[PrefixSuffix])){
      case ((string, prefixSuffix), (prefix, suffix)) =>
        if (prefixSuffix.isDefined)
          (string, prefixSuffix)
        else
          handlePrefixSuffix(string, prefix, suffix)
    }

  private def handlePrefixSuffix(
    string: String,
    prefixString: String,
    suffixString: String
  ): (String, Option[PrefixSuffix]) = {
    val prefixMatchCount = getPrefixMatchCount(string, prefixString)
    val suffixMatchCount = getSuffixMatchCount(string, suffixString)

    if (prefixMatchCount == 0 && suffixMatchCount == 0)
      (string, None)
    else {
      val prefix = prefixString * prefixMatchCount
      val suffix = suffixString * suffixMatchCount
      val expectedSuffix = suffixString * prefixMatchCount

      val item =
        if (prefix.equals(string)) {
          // the string is just prefix and suffix from the start to the end
          ""
        } else {
          string.substring(prefix.length, string.length - suffix.length).trim
        }
      (item, Some(PrefixSuffix(prefix, expectedSuffix, suffix)))
    }
  }

  private def getPrefixMatchCount(string: String, matchingString: String): Int =
    if (matchingString.isEmpty)
      0
    else
      string.grouped(matchingString.length).takeWhile(_.equals(matchingString)).size

  private def getSuffixMatchCount(string: String, matchingString: String) =
    getPrefixMatchCount(string.reverse, matchingString.reverse)

  private case class PrefixSuffix(
    prefix: String,
    expectedSuffix: String,
    suffix: String
  )

  private def fixImproperPrefixSuffix(
    delimiter: String,
    itemsWithPrefixSuffix: Array[(String, Option[PrefixSuffix])]
  ) = {
    val fixedItems = ListBuffer.empty[String]

    var unmatchedSuffixOption: Option[String] = None

    var bufferedItem = ""
    itemsWithPrefixSuffix.foreach{ case (item, prefixSuffix) =>
      val expectedSuffix = prefixSuffix.map(_.expectedSuffix).getOrElse("")
      val suffix = prefixSuffix.map(_.suffix).getOrElse("")

      unmatchedSuffixOption match {
        case None =>
          if (expectedSuffix.equals(suffix)) {
            // if we have both, prefix and suffix matching, everything is fine
            fixedItems += item
          } else {
            // prefix not matching suffix indicates an improper split, buffer
            unmatchedSuffixOption = Some(expectedSuffix)
            bufferedItem += item + delimiter
          }
        case Some(unmatchedSuffix) =>
          if (unmatchedSuffix.equals(suffix)) {
            // end buffering
            bufferedItem += item
            fixedItems += bufferedItem
            unmatchedSuffixOption = None
            bufferedItem = ""
          } else {
            // continue buffering
            bufferedItem += item + delimiter
          }
      }
    }
    if (unmatchedSuffixOption.isDefined) {
      throw new AdaParseException(s"Unmatched suffix detected ${unmatchedSuffixOption.get}.")
    }
    fixedItems
  }

  override def loadDataAndFields(
    dsa: DataSetAccessor,
    fieldNames: Seq[String],
    criteria: Seq[Criterion[Any]]
  ): Future[(Traversable[JsObject], Seq[Field])] = {
    val fieldsFuture =
      if (fieldNames.nonEmpty)
        dsa.fieldRepo.find(Seq(FieldIdentity.name #-> fieldNames))
      else
        dsa.fieldRepo.find()

    val dataFuture =
      if (fieldNames.nonEmpty)
        dsa.dataSetRepo.find(criteria, projection = fieldNames)
      else
        dsa.dataSetRepo.find(criteria)

    for {
      fields <- fieldsFuture
      jsons <- dataFuture
    } yield
      (jsons, fields.toSeq)
  }

  override def matchGroups(
    dataSetId: String,
    derivedDataSetSpec: ResultDataSetSpec,
    criteria: Seq[Criterion[Any]],
    targetGroupFieldName: String,
    confoundingFieldNames: Seq[String],
    numericDistTolerance: Double,
    targetGroupSelectionRatios: Seq[(String, Int)]
  ) = {
    val dsa = dsaf(dataSetId).getOrElse(throw new AdaException(s"Data Set ${dataSetId} not found."))

    // aux function to find group confounding values
    def findGroupConfoundingValues(
      fieldNameTypes: Seq[(String, FieldType[Any])])(
      groupValueSelectionRatio: (Any, Int)
    ): Future[(Int, ListBuffer[(BSONObjectID, Seq[Option[Any]])])] =
      dsa.dataSetRepo.find(
        criteria = criteria ++ Seq(targetGroupFieldName #== groupValueSelectionRatio._1),
        projection = confoundingFieldNames ++ Seq(JsObjectIdentity.name)
      ).map { jsons =>
        val idValues = jsons.map { json =>
          val id = (json \ JsObjectIdentity.name).as[BSONObjectID]
          (id, json.toValues(fieldNameTypes))
        }
        (groupValueSelectionRatio._2, ListBuffer(Random.shuffle(idValues).toSeq :_*))
      }

    // aux function to get the list of availble values (boolean, enum supported only)
    def availableValues(targetField: Field) = targetField.fieldType match {
      case FieldTypeId.Enum => targetField.enumValues.keys.toSeq
      case FieldTypeId.Boolean => Seq(true, false)
      case _ => throw new AdaException(s"Only enum and boolean types are allowed for a target group field. Got ${targetField.fieldType}.")
    }

    for {
      // confounding fields
      confoundingFields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> confoundingFieldNames)).map(_.toSeq)

      // confounding field names and types
      confoundingFieldNameTypes = confoundingFields.map(field => (field.name, ftf(field.fieldTypeSpec).asValueOf[Any]))

      // filter numberic field types
      numericFieldNameTypes = confoundingFieldNameTypes.filter { case (_, fieldType) => isNumeric(fieldType.spec.fieldType) }

      // min-maxes for numeric fields
      nameMinMaxMap <- Future.sequence(
        numericFieldNameTypes.map { case (fieldName, fieldType) =>
          minMaxDoubles(dsa.dataSetRepo, fieldName, fieldType, criteria).map(minMax => minMax.map((fieldName, _)))
        }
      ).map(_.flatten.toMap)

      // target field
      targetField <- dsa.fieldRepo.get(targetGroupFieldName).map(
        _.getOrElse(throw new AdaException(s"Target field $targetGroupFieldName not found."))
      )

      // target field type
      targetFieldType = ftf(targetField.fieldTypeSpec).asValueOf[Any]

      // group values with selection ratios
      groupValueSelectionRatios = targetGroupSelectionRatios match {
        case Nil => availableValues(targetField).map((_, 1)) // default selection is one sample per group
        case _ =>
          targetGroupSelectionRatios.flatMap { case (displayString, ratio) =>
            targetFieldType.displayStringToValue(displayString).map(value => (value, ratio))
          }
      }

      // for each group value find samples for all confounders
      selectCountConfoundingIdSamples <- Future.sequence {
        groupValueSelectionRatios.map(
          findGroupConfoundingValues(confoundingFieldNameTypes)
        )
      }

      // collect ids of the matched samples
      sampleIds = collectMatchedSampleIds(selectCountConfoundingIdSamples, confoundingFields, nameMinMaxMap, numericDistTolerance)

      // input stream (for given ids)
      inputStream <- dsa.dataSetRepo.findAsStream(Seq(JsObjectIdentity.name #-> sampleIds.toSeq))

      // get all the fields
      fields <- dsa.fieldRepo.find()

      // save the derived data set with an input stream (also save filters and data views)
      _ <- saveDerivedDataSet(dsa, derivedDataSetSpec, inputStream, fields.toSeq, StreamSpec(batchSize = Some(10)), true)
    } yield
      ()
  }

  private def minMaxDoubles[T](
    dataRepo: JsonReadonlyRepo,
    fieldName: String,
    fieldType: FieldType[_],
    criteria: Seq[Criterion[Any]]
  ): Future[Option[(Double, Double)]] = {
    val convert = doubleValue(fieldType.spec.fieldType)

    // aux function to convert to double
    def toDouble(jsValue: JsReadable): Option[Double] =
      convert(fieldType.asValueOf[Any].jsonToValue(jsValue))

    val maxFuture = dataRepo.max(fieldName, criteria, true).map(_.flatMap(toDouble))
    val minFuture = dataRepo.min(fieldName, criteria, true).map(_.flatMap(toDouble))

    for {
      min <- minFuture
      max <- maxFuture
    } yield
      (min, max).zipped.headOption
  }

  private def collectMatchedSampleIds(
    selectCountConfoundingIdSamples: Seq[(Int, ListBuffer[(BSONObjectID, Seq[Option[Any]])])],
    confoundingFields: Seq[Field],
    fieldNameMinMaxMap: Map[String, (Double, Double)],
    numericDistTolerance: Double
  ): Traversable[BSONObjectID] = {
    val nonEmptySelectCountConfoundingIdSamples = selectCountConfoundingIdSamples.filter(_._2.nonEmpty)

    if (nonEmptySelectCountConfoundingIdSamples.isEmpty) {
      Nil
    } else {
      nonEmptySelectCountConfoundingIdSamples.head._2.flatMap { case (id, matchingConfoundingSample) =>
        val matchedGroupIdSamples = nonEmptySelectCountConfoundingIdSamples.tail.map { case (selectCount, confoundingIdSamples) =>
          val bestSamples = findBestSampleMatches(matchingConfoundingSample, confoundingIdSamples, confoundingFields, fieldNameMinMaxMap, numericDistTolerance, selectCount)

          // only if the number of samples is as expected return them, otherwise return Nil indicating a failure
          if (bestSamples.size == selectCount) bestSamples else Nil
        }

        if (matchedGroupIdSamples.forall(_.nonEmpty)) {
          // remove matched samples
          selectCountConfoundingIdSamples.tail.zip(matchedGroupIdSamples).map {
            case ((_, confoundingIdSamples), matchedGroupIdSamples) =>
              confoundingIdSamples.--=(matchedGroupIdSamples)
          }

          // collect ids sand return
          val ids = matchedGroupIdSamples.flatten.map(_._1)
          Seq(id) ++ ids
        } else
          Nil
      }
    }
  }

  private def findBestSampleMatches(
    matchingConfoundingSample: Seq[Option[Any]],
    confoundingIdSamples: ListBuffer[(BSONObjectID, Seq[Option[Any]])],
    confoundingFields: Seq[Field],
    fieldNameMinMaxMap: Map[String, (Double, Double)],
    numericDistTolerance: Double,
    selectCount: Int
  ): Seq[(BSONObjectID, Seq[Option[Any]])] = {
    val matchedIdSamples = confoundingIdSamples.filter { case (_, confoundingSample) =>
      (matchingConfoundingSample, confoundingSample, confoundingFields).zipped.forall { case (matchingValue, value, field) =>
        (matchingValue.isEmpty && value.isEmpty) || field.isNumeric || (field.isCategorical && matchingValue.nonEmpty && value.nonEmpty && matchingValue.get == value.get)
      }
    }

    val idSampleDistances = matchedIdSamples.map { case (id, confoundingSample) =>
      val squareSum = (matchingConfoundingSample, confoundingSample, confoundingFields).zipped.map { case (matchingValue, value, field) =>
        if (field.isNumeric) {
          if (matchingValue.isEmpty && value.isEmpty)
            0d
          else {
            val (min, max) = fieldNameMinMaxMap.get(field.name).getOrElse(
              // not possible
              throw new IllegalArgumentException(s"Min max for a numeric field ${field.name} not found.")
            )

            val toDouble = {
              val convert = doubleValue(field.fieldType)
              (value: Option[Any]) => convert(value).getOrElse(Double.PositiveInfinity)
            }

            val normalizedDiff = Math.abs(toDouble(matchingValue) - toDouble(value)) / Math.abs(max - min)

            // square of normalized diff
            normalizedDiff * normalizedDiff
          }
        } else
          0d
      }.sum

      ((id, confoundingSample), Math.sqrt(squareSum))
    }.filter { case (_, distance) => distance < Math.min(numericDistTolerance, Double.PositiveInfinity) }

    idSampleDistances.sortBy(_._2).take(selectCount).map(_._1)
  }

  private def doubleValue(fieldTypeId: FieldTypeId.Value): Option[Any] => Option[Double] = {
    // aux double conversion function
    def toDouble[T](convert: T => Double)(value: Option[Any]): Option[Double] =
      value.asInstanceOf[Option[T]].map(convert)

    fieldTypeId match {
      case FieldTypeId.Double => toDouble[Double](identity)(_)
      case FieldTypeId.Integer => toDouble[Long](_.toDouble)(_)
      case FieldTypeId.Date => toDouble[java.util.Date](_.getTime.toDouble)(_)
      case _ => throw new IllegalArgumentException(s"Numeric type expected but got ${fieldTypeId}.")
    }
  }

  private def logProgress(index: Int, granularity: Double, total: Int) =
    if (index == total || (index % granularity) < ((index - 1) % granularity)) {
      val progress = if (total != 0) (index * 100) / total else 100
      val sb = new StringBuilder
      sb.append("Progress: [")
      for (_ <- 1 to progress)
        sb.append("=")
      for (_ <- 1 to 100 - progress)
        sb.append(" ")
      sb.append("]")
      logger.info(sb.toString)
    }
}