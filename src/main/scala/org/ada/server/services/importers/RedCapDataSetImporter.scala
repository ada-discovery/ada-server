package org.ada.server.services.importers

import java.util.Date

import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.CategoryRepo
import org.ada.server.dataaccess.RepoTypes.FieldRepo
import org.ada.server.dataaccess._
import org.ada.server.models.{Category, Field, FieldTypeId, FieldTypeSpec}
import org.ada.server.field.FieldType
import org.ada.server.models.redcap.{Metadata, FieldType => RCFieldType}
import org.ada.server.models.dataimport.RedCapDataSetImport
import org.ada.server.{AdaException, AdaParseException}
import org.ada.server.field.FieldUtil.FieldOps
import org.incal.core.dataaccess.Criterion.Infix
import org.incal.core.util.{hasNonAlphanumericUnderscore, seqFutures}

import play.api.libs.json._
import reactivemongo.bson.BSONObjectID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class RedCapDataSetImporter @Inject() (
    redCapServiceFactory: RedCapServiceFactory
  ) extends AbstractDataSetImporter[RedCapDataSetImport] {

  private val choicesDelimiter = "\\|"
  private val choiceKeyValueDelimiter = ","
  private val visitFieldName = "redcap_event_name"
  private val visitLabel = "Visit"
  private val visitPrefix = "visit"
  private val armPrefix = "arm"

  private val defaultSaveBatchSize = 20

  override def runAsFuture(importInfo: RedCapDataSetImport): Future[Unit] = {
    logger.info(new Date().toString)

    if (importInfo.importDictionaryFlag)
      logger.info(s"Import of data set and dictionary '${importInfo.dataSetName}' initiated.")
    else
      logger.info(s"Import of data set '${importInfo.dataSetName}' initiated.")

    // Red cap service to pull the data from
    val redCapService = redCapServiceFactory(importInfo.url, importInfo.token)

    val stringFieldType = ftf.stringScalar

    val batchSize = importInfo.saveBatchSize.getOrElse(defaultSaveBatchSize)

    // helper functions to parse jsons
    def displayJsonToJson[T](fieldType: FieldType[T], json: JsReadable): JsValue = {
      val value = fieldType.displayJsonToValue(json)
      fieldType.valueToJson(value)
    }

    def displayJsonToJsonRedcapEnum(json: JsReadable) =
      stringFieldType.displayJsonToValue(json) match {
        case Some(string) => JsNumber(string.toInt)
        case None => JsNull
      }

    for {
      // get the data from a given red cap service
      records <- {
        logger.info("Downloading records from REDCap...")
        if (importInfo.eventNames.nonEmpty) {
          seqFutures(importInfo.eventNames) { eventName =>
            logger.info(s"Downloading records for the event ${eventName}...")
            redCapService.listEventRecords(Seq(eventName.trim))
          }.map(_.flatten)
        } else
          redCapService.listAllRecords // "cdisc_dm_usubjd"
      }

      // create/retrieve a dsa
      dsa <- createDataSetAccessor(importInfo)

      fieldRepo = dsa.fieldRepo
      categoryRepo = dsa.categoryRepo

      // import dictionary (if needed) otherwise use an existing one (should exist)
      fieldsWithEnumFlag <- importOrGetDictionary(importInfo, redCapService, fieldRepo, categoryRepo, records)
      fields = fieldsWithEnumFlag.map(_._1)

      // create a name -> field type (+ redcap enum flag) map for a quick lookup
      fieldNameTypeMap = {
        val map: Map[String, (FieldType[_], Boolean)] = fieldsWithEnumFlag.map { case (field, redcapEnumFlag) =>
          (field.name, (ftf(field.fieldTypeSpec): FieldType[_], redcapEnumFlag))
        }.toSeq.toMap

        map
      }

      // get the ids of the categories that need to be inherited from the first visit
      categoryIdsToInherit <-
        if (importInfo.categoriesToInheritFromFirstVisit.nonEmpty)
          categoryRepo.find(Seq("name" #-> importInfo.categoriesToInheritFromFirstVisit)).map(_.map(_._id.get))
        else
          Future(Nil)

      // obtain the names of the fiels that need to be inherited
      fieldNamesToInherit = {
        val categoryIdsToInheritSet = categoryIdsToInherit.toSet

        val fieldsToInherit = fields.filter(
          _.categoryId.map(categoryIdsToInheritSet.contains(_)).getOrElse(false)
        )

        fieldsToInherit.map(_.name)
      }

      // since we possible changed the dictionary (the data structure) we need to update the data set repo
      _ <- dsa.updateDataSetRepo

      // get the new data set repo
      dataRepo = dsa.dataSetRepo

      // delete all the records
      _ <- {
        logger.info(s"Deleting the old data set...")
        dataRepo.deleteAll
      }

      // get the records with inferred types
      newRecords: Traversable[JsObject] = records.map { record =>
        val newJsValues = record.fields.map { case (fieldName, jsValue) =>
          val newJsValue = fieldNameTypeMap.get(fieldName) match {

            case Some((fieldType, redcapEnumType)) => try {
              if (redcapEnumType)
                displayJsonToJsonRedcapEnum(jsValue)
              else
                displayJsonToJson(fieldType, jsValue)
            } catch {
              case e: Exception => throw new AdaException(s"JSON value '$jsValue' of the field '$fieldName' cannot be processed.", e)
            }

            // TODO: this shouldn't be like that... if we don't have a field in the dictionary, we should discard it
            case None => jsValue
          }
          (fieldName, newJsValue)
        }

        JsObject(newJsValues)
      }

      // inherit the values
      inheritedRecords =
        importInfo.setting.map { setting =>
          if (fieldNamesToInherit.nonEmpty)
            inheritFieldValues(newRecords, setting.keyFieldName, fieldNameTypeMap, fieldNamesToInherit)
          else
            newRecords
        }.getOrElse(
          newRecords
        )

      // save the records
      _ <- dataSetService.saveOrUpdateRecords(dataRepo, inheritedRecords.toSeq, batchSize = Some(batchSize))
    } yield
      ()
  }

  private def importOrGetDictionary(
    importInfo: RedCapDataSetImport,
    redCapService: RedCapService,
    fieldRepo: FieldRepo,
    categoryRepo: CategoryRepo,
    records: Traversable[JsObject]
  ): Future[Traversable[(Field, Boolean)]] = {
    if (importInfo.importDictionaryFlag) {
      logger.info(s"RedCap dictionary inference and import for data set '${importInfo.dataSetId}' initiated.")

      val fieldsFuture = importAndInferRedCapDictionary(importInfo.dataSetId, redCapService, fieldRepo, categoryRepo, records)

      fieldsFuture.map { fields =>
        logger.info(s"RedCap dictionary inference and import for data set '${importInfo.dataSetId}' successfully finished.")
        fields
      }
    } else {
      logger.info(s"RedCap dictionary import disabled, using an existing dictionary.")

      for {
        // existing fields
        fields <- fieldRepo.find()

        // obtain the RedCAP metadata
        metadatas <- redCapService.listMetadatas
      } yield
        if (fields.nonEmpty) {
          val redCapFieldNameMap = metadatas.flatMap(metadata => getRedCapFixedType(metadata).map((metadata.field_name, _))).toMap

          // fields with enum flag
          fields.map { field =>
            val redcapEnum = redCapFieldNameMap.get(field.name).map { case (fieldType, isRedCapEnum) =>
              if (field.isEnum) {
                // TODO: we should do some comparison / validation + enum values update
                //                field.enumValues.get.toSeq.sorted
                isRedCapEnum
              } else
                false

            }.getOrElse(false)

            (field, redcapEnum)
          }
        } else {
          val message = s"No dictionary found for the data set '${importInfo.dataSetId}'. Run the REDCap data set import again with the 'import dictionary' option."
          logger.error(message)
          throw new AdaException(message)
        }
    }
  }

  private def inheritFieldValues(
    records: Traversable[JsObject],
    keyFieldName: String,
    fieldNameTypeMap: Map[String, (FieldType[_], Boolean)],
    fieldNamesToInherit: Traversable[String]
  ): Traversable[JsObject] = {
    logger.info("Inheriting fields from the first visit...")

    def getFieldType(fieldName: String) =
      fieldNameTypeMap.get(fieldName).map(_._1).getOrElse(
        throw new AdaException(s"Field $fieldName not found in the dictionary.")
      )

    val visitField = getFieldType(visitFieldName)

    def visitValue(json: JsObject): String =
      visitField.jsonToDisplayString(json \ visitFieldName)

    def inheritFields(
      json: JsObject,
      visit1Json: JsObject
    ): JsObject = {
      val inheritedJsValues = fieldNamesToInherit.map { fieldName =>
//        val fieldType = getFieldType(fieldName)
        val jsValue = (json \ fieldName).toOption.getOrElse(JsNull)

        val inheritedJsValue =
          if (jsValue == JsNull)
            (visit1Json \ fieldName).get
          else
            jsValue

        (fieldName, inheritedJsValue)
      }.toSeq

      json.++(JsObject(inheritedJsValues))
    }

    val visit1Id = s"${visitPrefix}_1_${armPrefix}_1"
    val keyField = getFieldType(keyFieldName)

    records.groupBy(json => keyField.jsonToValue(json \ keyFieldName)).map { case (_, groupRecords) =>

      val visit1RecordOption = groupRecords.find( record => visitValue(record).equals(visit1Id) )

      visit1RecordOption.map { visit1Record =>
        groupRecords.map { record =>
          if (!visitValue(record).equals(visit1Id))
            inheritFields(record, visit1Record)
          else
            record
        }
      }.getOrElse(groupRecords)
    }.flatten
  }

  private def importAndInferRedCapDictionary(
    dataSetId: String,
    redCapService: RedCapService,
    fieldRepo: FieldRepo,
    categoryRepo: CategoryRepo,
    records: Traversable[JsObject]
  ): Future[Traversable[(Field, Boolean)]] = {

    def displayJsonToDisplayString[T](fieldType: FieldType[T], json: JsReadable): String = {
      val value = fieldType.displayJsonToValue(json)
      fieldType.valueToDisplayString(value)
    }

    val fieldNames = records.map(_.keys).flatten.toSet
    val stringFieldType = ftf.stringScalar
    val inferredFieldNameTypeMap: Map[String, FieldType[_]] =
      fieldNames.map { fieldName =>
        val stringValues = records.map(record =>
          displayJsonToDisplayString(stringFieldType, (record \ fieldName))
        )
        (fieldName, defaultFti(stringValues))
      }.toMap

    // TODO: optimize this... introduce field groups to speed up inference
    def inferDictionary(
      metadatas: Seq[Metadata],
      nameCategoryIdMap: Map[String, BSONObjectID]
    ): Traversable[(Field, Boolean)] = {
      val inferredFieldNames = inferredFieldNameTypeMap.keySet
      val inferredFieldNamePrefixes = inferredFieldNames.flatMap { name =>
        val index = name.indexOf("___")
        if (index > 0) Some(name.substring(0, index)) else None
      }

      logger.info(inferredFieldNamePrefixes.mkString("\n"))

      metadatas.filter { metadata =>
        metadata.field_type match {
          case RCFieldType.checkbox =>
            inferredFieldNamePrefixes.contains(metadata.field_name)
          case _ =>
            inferredFieldNames.contains(metadata.field_name)
        }
      }.par.flatMap { metadata =>
        val fieldName = metadata.field_name

        // check if a field name is legal
        if (hasNonAlphanumericUnderscore(fieldName)) {
          throw new AdaParseException(s"The REDCap field name ${fieldName} is illegal since it contain some non-alphanumeric characters (except underscore).")
        }

        val categoryId = nameCategoryIdMap.get(metadata.form_name)

        if (metadata.field_type != RCFieldType.checkbox) {
          def inferredType = {
            val inferredFieldType: FieldType[_] = inferredFieldNameTypeMap.get(fieldName).get
            (inferredFieldType.spec, false)
          }

          val (fieldTypeSpec, isRedCapEnum) = getRedCapFixedType(metadata).getOrElse(inferredType)

          val stringEnumValues = fieldTypeSpec.enumValues.map { case (from, to) => (from.toString, to) }
          val field = Field(fieldName, Some(metadata.field_label), fieldTypeSpec.fieldType, fieldTypeSpec.isArray, stringEnumValues, None, None, None, Nil, categoryId)
          Seq((field, isRedCapEnum))
        } else {
          val choices = getEnumValues(metadata)
          choices.map { case (suffix, label) =>
            val field = Field(s"${fieldName}___$suffix", Some(metadata.field_label + " " + label), FieldTypeId.Boolean, categoryId = categoryId)
            (field, false)
          }
        }
      }.toList
    }

    for {
      // obtain the RedCAP metadata
      metadatas <- redCapService.listMetadatas // "field_name"

      // save the obtained categories and return category names with ids
      categoryNameIds <- Future.sequence {
        metadatas.map(_.form_name).toSet.map { categoryName: String =>
          categoryRepo.find(Seq("name" #== categoryName)).flatMap { categories =>
            val id: Future[BSONObjectID] =
              categories.headOption match {
                case Some(category) => Future(category._id.get)
                case None => categoryRepo.save(new Category(categoryName))
              }

            id.map((categoryName, _))
          }
        }
      }

      // fields
      newFieldsWithEnumFlag = {
        val fields = inferDictionary(metadatas, categoryNameIds.toMap)

        // also add redcap_event_name
        inferredFieldNameTypeMap.get(visitFieldName).map(_.spec) match {
          case Some(visitFieldTypeSpec) =>
            val stringEnums = visitFieldTypeSpec.enumValues.map { case (from, to) => (from.toString, to) }
            val visitField = Field(visitFieldName, Some(visitLabel), visitFieldTypeSpec.fieldType, visitFieldTypeSpec.isArray, stringEnums)
            fields ++ Seq((visitField, false))

          case None => fields
        }
      }

      newFields = newFieldsWithEnumFlag.map(_._1)

      // save the fields
      _ <- dataSetService.updateFields(dataSetId, newFields, true, true)
    } yield
      newFieldsWithEnumFlag
  }

  private def getRedCapFixedType(
    metadata: Metadata
  ): Option[(FieldTypeSpec, Boolean)] = {
    val fieldSpecType = metadata.field_type match {
      case RCFieldType.radio => Some(enumOrDoubleOrString(metadata))
      case RCFieldType.dropdown => Some(enumOrDoubleOrString(metadata))
      case RCFieldType.yesno => Some(FieldTypeSpec(FieldTypeId.Boolean))
      case RCFieldType.truefalse => Some(FieldTypeSpec(FieldTypeId.Boolean))
      case _ => None
    }

    fieldSpecType.map(spec => (spec, spec.fieldType == FieldTypeId.Enum))
  }

  private def enumOrDoubleOrString(
    metadata: Metadata
  ): FieldTypeSpec =
    try {
      FieldTypeSpec(FieldTypeId.Enum, false, getEnumValues(metadata))
    } catch {
      case e: AdaParseException =>
        try {
          getDoubles(metadata)
          logger.warn(s"The field '${metadata.field_name}' has floating part(s) in the enum list and so will be treated as Double.")
          FieldTypeSpec(FieldTypeId.Double)
        } catch {
          case e: AdaParseException =>
            logger.warn(s"The field '${metadata.field_name}' has strings in the enum list and so will be treated as String.")
            FieldTypeSpec(FieldTypeId.String)
        }
    }

  private def getEnumValues(metadata: Metadata): Map[Int, String] = {
    val choices = metadata.select_choices_or_calculations.trim

    if (choices.nonEmpty) {
      try {
        choices.split(choicesDelimiter).map { choice =>
          val keyValueString = choice.split(choiceKeyValueDelimiter, 2)

          val stringKey = keyValueString(0).trim
          val value = keyValueString(1).trim

          (stringKey.toInt, value)
        }.toMap
      } catch {
        case e: NumberFormatException => throw new AdaParseException(s"RedCap Metadata '${metadata.field_name}' has non-parseable choices '${metadata.select_choices_or_calculations}'.")
      }
    } else
      Map()
  }

  private def getDoubles(metadata: Metadata): Option[Traversable[Double]] = {
    val choices = metadata.select_choices_or_calculations.trim

    if (choices.nonEmpty) {
      try {
        val doubles = choices.split(choicesDelimiter).map { choice =>
          val keyValueString = choice.split(choiceKeyValueDelimiter, 2)

          val stringKey = keyValueString(0).trim
          val value = keyValueString(1).trim

          stringKey.toDouble
        }
        Some(doubles)
      } catch {
        case e: NumberFormatException => throw new AdaParseException(s"RedCap Metadata '${metadata.field_name}' has non-parseable choices '${metadata.select_choices_or_calculations}'.")
      }
    } else
      None
  }
}