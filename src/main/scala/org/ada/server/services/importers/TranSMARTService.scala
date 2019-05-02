package org.ada.server.services.importers

import org.ada.server.AdaException
import org.ada.server.dataaccess.AdaConversionException
import org.ada.server.dataaccess.JsonUtil.jsonsToCsv
import org.ada.server.field.{FieldType, FieldTypeHelper}
import org.ada.server.models.{Category, Field}
import play.api.libs.json._

import com.google.inject.ImplementedBy
import javax.inject.Singleton

@ImplementedBy(classOf[TranSMARTServiceImpl])
trait TranSMARTService {

  /**
    * Creates a template for the clinical mapping
    *
    * @param dataFileName Name of output file.
    * @param keyField Field for use as unique key for tranSMART mapping file.
    * @param visitField Field to be used as visit field in tranSMART mapping file.
    * @param fieldsInOrder Filtered input fields.
    * @param fieldCategoryMap Define which field names map to which tranSMART categories.
    * @param rootCategory Category to be used as tranSMART root.
    * @param fieldNameLabelMap (Re)map field to labels in tranSMART file.
    * @return Items containing the values for the clinical mapping file.
    */
  def createClinicalMapping(
    dataFileName: String,
    keyField: String,
    visitField: Option[String],
    fieldsInOrder: Iterable[String],
    fieldCategoryMap: Map[String, Category],
    rootCategory: Category,
    fieldNameLabelMap: Map[String, Option[String]]
  ) : Traversable[JsObject]

  /**
    * Deprecated.
    * Process input items to create clinical and mapping file.
    * Replace substrings and symbols if necessary, map columns to tranSMART properties and fields.
    *
    * @see createClinicalData()
    * @see createClinicalMapping()
    * @param delimiter String to use as entry delimiter.
    * @param newLine String to use as line delimiter.
    * @param replacements List of pairs for replacing strings and symbols of format: (input string, replacement).
    * @param items Items to be written into output file. May be modified with respect to the other parameters.
    * @param dataFileName Name of output file.
    * @param keyField Field for use as unique key for tranSMART mapping file.
    * @param visitField Field to be used as visit field in tranSMART mapping file.
    * @param fieldCategoryMap Define which field names map to which tranSMART categories.
    * @param rootCategory Category to be used as tranSMART root.
    * @param fields The list of fields.
    * @return Pair containing the content for the tranSMART datafile and the tranSMART mapping file.
    */
  def createClinicalDataAndMappingFiles(
    delimiter : String,
    newLine : String,
    replacements : Iterable[(String, String)]
  )(
    items: Traversable[JsObject],
    dataFileName: String,
    keyField: String,
    visitField: Option[String],
    fieldCategoryMap: Map[String, Category],
    rootCategory: Category,
    fields: Traversable[Field]
  ): (String, String)

  /**
    * Generates the content for the clinical data file.
    * Replace substrings and symbols if necessary, map columns to tranSMART properties and fields.
    *
    * @param delimiter String to use as entry delimiter.
    * @param newLine String to use as line delimiter.
    * @param replacements List of pairs for replacing strings and symbols of format: (input string, replacement).
    * @param items Items to be written into output file. May be modified with respect to the other parameters.
    * @param keyFieldName Field for use as unique key for tranSMART mapping file.
    * @param visitFieldName Field to be used as visit field in tranSMART mapping file.
    * @param fieldNameCategoryMap Define which field names map to which tranSMART categories.
    * @return
    */
  def createClinicalDataFile(
    delimiter: String,
    newLine: String,
    replacements: Iterable[(String, String)]
  )(
    items: Traversable[JsObject],
    keyFieldName: String,
    visitFieldName: Option[String],
    fieldNameCategoryMap: Map[String, Category],
    nameFieldTypeMap: Map[String, FieldType[_]]
  ): String

  /**
    * Generate the content for the data mapping file.
    * Replace substrings and symbols if necessary, map columns to tranSMART properties and fields.
    *
    * @param delimiter String to use as entry delimiter.
    * @param newLine String to use as line delimiter.
    * @param replacements List of pairs for replacing strings and symbols of format: (input string, replacement).
    * @param dataFileName Name of output file.
    * @param keyField Field for use as unique key for tranSMART mapping file.
    * @param visitField Field to be used as visit field in tranSMART mapping file.
    * @param fieldCategoryMap Define which field names map to which tranSMART categories.
    * @param rootCategory Category to be used as tranSMART root.
    * @param fieldNameLabelMap (Re)map field to labels in tranSMART file.
    * @return
    */
  def createMappingFile(
    delimiter : String,
    newLine : String,
    replacements : Iterable[(String, String)]
  )(
    dataFileName: String,
    keyField: String,
    visitField: Option[String],
    fieldCategoryMap: Map[String, Category],
    rootCategory: Category,
    fieldNameLabelMap: Map[String, Option[String]]
  ): String
}

@Singleton
class TranSMARTServiceImpl extends TranSMARTService {

  private val ftf = FieldTypeHelper.fieldTypeFactory()

  override def createClinicalMapping(
    dataFileName: String,
    keyField: String,
    visitField: Option[String],
    fieldsInOrder: Iterable[String],
    fieldCategoryMap: Map[String, Category],
    rootCategory: Category,
    fieldNameLabelMap: Map[String, Option[String]]
   ) = {
    fieldsInOrder.zipWithIndex.map{ case (fieldName, index) =>
      val (label, path) = if (fieldName.equals(keyField))
        (JsString("SUBJ_ID"), None)
      else if (visitField.isDefined && visitField.get.equals(fieldName))
        (JsString("VISIT_ID"), None)
      else {
        val label = JsString(fieldNameLabelMap.get(fieldName).flatten.getOrElse(fieldName))
        val path = fieldCategoryMap.get(fieldName).map(_.getLabelPath.mkString("+").replaceAll(" ", "_"))
        (label, path)
      }

      JsObject(
        List(
          ("filename", JsString(dataFileName)),
          ("category_cd", if (path.isDefined) JsString(path.get) else JsNull),
          ("col_nbr", Json.toJson(index + 1)),
          ("data_label", label)
        )
      )
    }
  }

  override def createClinicalDataAndMappingFiles(
    delimiter : String,
    newLine : String,
    replacements : Iterable[(String, String)]
  )(
    items: Traversable[JsObject],
    dataFileName: String,
    keyField: String,
    visitField: Option[String],
    fieldCategoryMap: Map[String, Category],
    rootCategory: Category,
    fields: Traversable[Field]
  ) = {
    val fieldsToIncludeInOrder: Seq[String] = (
      (
        if (visitField.isDefined)
          Seq(keyField, visitField.get)
        else
          Seq(keyField)
      ) ++
        fieldCategoryMap.keys.filterNot(_.equals(keyField)).toSeq.sorted
    )

    val fieldNameLabelMap = fields.map( field => (field.name, field.label)).toMap
    val nameFieldTypeMap: Map[String, FieldType[_]] = fields.map( field => (field.name, ftf(field.fieldTypeSpec))).toMap

    val clinicalData = createClinicalData(items, fieldsToIncludeInOrder, nameFieldTypeMap)
    if (clinicalData.nonEmpty) {
      val mappingData = createClinicalMapping(dataFileName, keyField, visitField, fieldsToIncludeInOrder, fieldCategoryMap, rootCategory, fieldNameLabelMap)

      val dataContent = jsonsToCsv(clinicalData, delimiter, newLine, Nil, replacements)
      val mappingContent = jsonsToCsv(mappingData, delimiter, newLine, Nil, replacements)
      (dataContent, mappingContent)
    } else
      ("" , "")
  }


  def createClinicalDataFile(
    delimiter : String,
    newLine : String,
    replacements : Iterable[(String, String)]
  )(
    items : Traversable[JsObject],
    keyFieldName : String,
    visitFieldName : Option[String],
    fieldCategoryMap : Map[String, Category],
    nameFieldTypeMap: Map[String, FieldType[_]]
  ) = {
    val fieldsToIncludeInOrder: Seq[String] = (
      (
        if (visitFieldName.isDefined)
          Seq(keyFieldName, visitFieldName.get)
        else
          Seq(keyFieldName)
        ) ++
        fieldCategoryMap.keys.filterNot(_.equals(keyFieldName)).toSeq.sorted
      )

    val clinicalData = createClinicalData(items, fieldsToIncludeInOrder, nameFieldTypeMap)
    if (clinicalData.nonEmpty) {
      val dataContent = jsonsToCsv(clinicalData, delimiter, newLine, Nil, replacements)
      dataContent
    } else
      ""
  }

  def createMappingFile(
    delimiter : String,
    newLine : String,
    replacements : Iterable[(String, String)]
  )(
    dataFileName : String,
    keyField : String,
    visitField : Option[String],
    fieldCategoryMap : Map[String, Category],
    rootCategory : Category,
    fieldNameLabelMap: Map[String, Option[String]]
   ) = {
    val fieldsToIncludeInOrder: Seq[String] = (
      (
        if (visitField.isDefined)
          Seq(keyField, visitField.get)
        else
          Seq(keyField)
        ) ++
        fieldCategoryMap.keys.filterNot(_.equals(keyField)).toSeq.sorted
      )

    if (fieldsToIncludeInOrder.nonEmpty) {
      val mappingData = createClinicalMapping(dataFileName, keyField, visitField, fieldsToIncludeInOrder, fieldCategoryMap, rootCategory, fieldNameLabelMap)

      val mappingContent = jsonsToCsv(mappingData, delimiter, newLine, Nil, replacements)
      mappingContent
    } else
      ""
  }

  /**
    * Takes fields and filters them according to parameters by either including or excluding fields.
    * Be aware that this call can not be made with both fieldsToInclude and fieldsToExclude being defined at the same time.
    *
    * @param items Input items to be filtered.
    * @param fieldsToInclude List of fields to be used for result generation.
    * @return Items with defined entries included/ excluded.
    */
  private def createClinicalData(
    items: Traversable[JsObject],
    fieldsToInclude: Seq[String],
    nameFieldTypeMap: Map[String, FieldType[_]]
  ) = {
    items.map { item =>
      val fieldNameJsonMap = item.fields.toMap

      val filteredJsons = fieldsToInclude.map { fieldName =>
        val json = fieldNameJsonMap.get(fieldName).getOrElse(
          throw new IllegalArgumentException(s"Field name '$fieldName' not found in the TranSMART export json.")
        )

        val displayJson =
          json match {
            case JsNull => JsNull
            case _ => {
              nameFieldTypeMap.get(fieldName).map { fieldType =>
                try {
                  JsString(fieldType.jsonToDisplayString(json))
                } catch {
                  case e: AdaConversionException =>
                    throw new AdaException(s"String conversion of the field $fieldName failed", e)
                }
              }.getOrElse(json)
            }
          }

        (fieldName, displayJson)
      }
      JsObject(filteredJsons)
    }
  }
}