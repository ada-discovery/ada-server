package org.ada.server.services.importers

import java.util.Date

import org.ada.server.models.dataimport.TranSmartDataSetImport
import org.ada.server.AdaParseException
import org.ada.server.dataaccess.dataset.CategoryRepo._
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.dataaccess.RepoTypes.{CategoryRepo, FieldRepo}
import org.ada.server.models.{Category, Field}
import org.ada.server.dataaccess.dataset.DataSetAccessor
import org.ada.server.field.inference.FieldTypeInferrerFactory
import reactivemongo.bson.BSONObjectID
import org.incal.core.util.seqFutures

import collection.mutable.{Map => MMap}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

private class TranSmartDataSetImporter extends AbstractDataSetImporter[TranSmartDataSetImport] {

  private val tranSmartDelimeter = '\t'
  private val tranSmartFieldGroupSize = 100

  private val quotePrefixSuffix = ("\"", "\"")

  override def runAsFuture(importInfo: TranSmartDataSetImport): Future[Unit] = {
    logger.info(new Date().toString)
    logger.info(s"Import of data set '${importInfo.dataSetName}' initiated.")

    val delimiter = tranSmartDelimeter.toString

    try {
      val lines = createCsvFileLineIterator(
        importInfo.dataPath.get,
        importInfo.charsetName,
        None
      )

      // collect the column names and labels
      val columnNamesAndLabels = dataSetService.getColumnNameLabels(delimiter, lines)

      // parse lines
      logger.info(s"Parsing lines...")
      val prefixSuffixSeparators = if (importInfo.matchQuotes) Seq(quotePrefixSuffix) else Nil
      val values = dataSetService.parseLines(columnNamesAndLabels.size, lines, delimiter, false, prefixSuffixSeparators)

      for {
        // create/retrieve a dsa
        dsa <- createDataSetAccessor(importInfo)

        // save the jsons and dictionary
        _ <-
          if (importInfo.inferFieldTypes)
            saveJsonsWithTypeInference(dsa, columnNamesAndLabels, values, importInfo)
          else
            saveJsonsWithoutTypeInference(dsa, columnNamesAndLabels, values, importInfo)
      } yield
        ()
    } catch {
      case e: Exception => Future.failed(e)
    }
  }

  private def saveJsonsWithoutTypeInference(
    dsa: DataSetAccessor,
    columnNamesAndLabels: Seq[(String, String)],
    values: Iterator[Seq[String]],
    importInfo: TranSmartDataSetImport
  ): Future[Unit] = {
    // create jsons and field types
    logger.info(s"Creating JSONs...")
    val (jsons, fields) = createJsonsWithStringFields(columnNamesAndLabels, values)

    for {
      // save, or update the dictionary
      _ <- {
        if (importInfo.mappingPath.isDefined) {
          importTranSMARTDictionary(
            importInfo.dataSetId,
            dsa.fieldRepo,
            dsa.categoryRepo,
            tranSmartFieldGroupSize,
            tranSmartDelimeter.toString,
            createCsvFileLineIterator(
              importInfo.mappingPath.get,
              importInfo.charsetName,
              None
            ),
            fields
          )
        } else {
          dataSetService.updateFields(dsa.fieldRepo, fields, true, true)
        }
      }

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

        importInfo.saveBatchSize match {
          case Some(saveBatchSize) =>
            seqFutures(jsons.grouped(saveBatchSize))(dataRepo.save)

          case None =>
            Future.sequence(jsons.map(dataRepo.save))
        }
      }
    } yield
      ()
  }

  private def saveJsonsWithTypeInference(
    dsa: DataSetAccessor,
    columnNamesAndLabels: Seq[(String, String)],
    values: Iterator[Seq[String]],
    importInfo: TranSmartDataSetImport
  ): Future[Unit] = {

    // infer field types and create JSONSs
    logger.info(s"Inferring field types and creating JSONs...")
    val fti =
      if (importInfo.inferenceMaxEnumValuesCount.isDefined || importInfo.inferenceMinAvgValuesPerEnum.isDefined) {
        Some(
          new FieldTypeInferrerFactory(
            FieldTypeHelper.fieldTypeFactory(),
            importInfo.inferenceMaxEnumValuesCount.getOrElse(FieldTypeHelper.maxEnumValuesCount),
            importInfo.inferenceMinAvgValuesPerEnum.getOrElse(FieldTypeHelper.minAvgValuesPerEnum),
            FieldTypeHelper.arrayDelimiter
          ).ofString
        )
      } else
        None

    val (jsons, fields) = createJsonsWithFields(columnNamesAndLabels, values.toSeq, fti)

    for {
    // save, or update the dictionary
      _ <- {
        if (importInfo.mappingPath.isDefined) {
          importTranSMARTDictionary(
            importInfo.dataSetId,
            dsa.fieldRepo,
            dsa.categoryRepo,
            tranSmartFieldGroupSize,
            tranSmartDelimeter.toString,
            createCsvFileLineIterator(
              importInfo.mappingPath.get,
              importInfo.charsetName,
              None
            ),
            fields
          )
        } else {
          dataSetService.updateFields(dsa.fieldRepo, fields, true, true)
        }
      }

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
        dataSetService.saveOrUpdateRecords(dataRepo, jsons,  None, false, None, importInfo.saveBatchSize)
      }
    } yield
      ()
  }

  protected def importTranSMARTDictionary(
    dataSetId: String,
    fieldRepo: FieldRepo,
    categoryRepo: CategoryRepo,
    fieldGroupSize: Int,
    delimiter: String,
    mappingFileLineIterator: => Iterator[String],
    fields: Seq[Field]
  ): Future[Unit] = {
    logger.info(s"TranSMART dictionary inference and import for data set '${dataSetId}' initiated.")

    // read the mapping file to obtain tuples: field name, field label, and category name; and a category name map
    val indexFieldNameMap: Map[Int, String] = fields.map(_.name).zipWithIndex.map(_.swap).toMap

    val (fieldNameLabelCategoryMap, categories) = createFieldLabelCategoryMap(mappingFileLineIterator, delimiter, indexFieldNameMap)

    for {
      // delete all the fields
      _ <- fieldRepo.deleteAll

      // delete all the categories
      _ <- categoryRepo.deleteAll

      // save the categories
      categoryIds: Traversable[(Category, BSONObjectID)] <- {
        val firstLayerCategories = categories.filter(!_.parent.isDefined)
        Future.sequence(
          firstLayerCategories.map(
            saveRecursively(categoryRepo, _)
          )
        ).map(_.flatten)
      }

      // save the fields... use a field label and a category from the mapping file provided, and infer a type
      _ <- {
        val categoryIdMap = categoryIds.toMap

        val newFields = fields.map { field =>
          val (fieldLabel, category) = fieldNameLabelCategoryMap.getOrElse(field.name, ("", None))
          val categoryId = category.map(categoryIdMap.get(_).get)

          field.copy(label = Some(fieldLabel), categoryId = categoryId)
        }

        dataSetService.updateFields(fieldRepo, newFields, true, true)
      }
    } yield
      logger.info(s"TranSMART dictionary inference and import for data set '${dataSetId}' successfully finished.")
  }

  private def createFieldLabelCategoryMap(
    lineIterator: => Iterator[String],
    delimiter: String,
    indexFieldNameMap: Map[Int, String]
  ): (Map[String, (String, Option[Category])], Seq[Category]) = {
    val pathCategoryMap = MMap[String, Category]()
//    val columnNames  =  dataSetService.getColumnNames(delimiter, lineIterator)

    val fieldNameLabelCategoryMap = lineIterator.drop(1).zipWithIndex.map { case (line, index) =>
      val values = dataSetService.parseLine(delimiter, line)
      if (values.size < 4)
        throw new AdaParseException(s"TranSMART mapping file contains a line (index '$index') with '${values.size}' items, but 4 expected (filename, category, column number, and data label). Parsing terminated.")

      val filename	= values(0).trim
      val categoryCD	= values(1).trim
      val colNumber	= values(2).trim.toInt
      val fieldLabel = values(3).trim

      val fieldName = indexFieldNameMap.get(colNumber - 1).getOrElse(
        throw new AdaParseException(s"TranSMART mapping file contains an invalid reference to a non-existing column '$colNumber.' Parsing terminated.")
      )

      // collect all categories
      val categoryPathNames = if (categoryCD.nonEmpty)
        categoryCD.split("\\+").map(_.trim.replaceAll("_", " "))
      else
        Array[String]()

      val assocCategory =
        if (categoryPathNames.nonEmpty) {
          val categories = (1 to categoryPathNames.length).map { pathSize =>
            val path = categoryPathNames.take(pathSize)
            val categoryName = path.last
            pathCategoryMap.getOrElseUpdate(path.mkString("+"), new Category(categoryName))
          }
          if (categories.size > 1) {
            categories.sliding(2).foreach { adjCategories =>
              val parent = adjCategories(0)
              val child = adjCategories(1)
              if (!parent.children.contains(child))
               parent.addChild(child)
            }
          }
          Some(categories.last)
        } else
          None

      (fieldName, (fieldLabel, assocCategory))
    }.toMap
    (fieldNameLabelCategoryMap, pathCategoryMap.values.toSeq)
  }
}
