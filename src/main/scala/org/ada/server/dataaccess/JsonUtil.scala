package org.ada.server.dataaccess

import java.util.regex.Pattern

import play.api.libs.json.{JsObject, _}

object JsonUtil {

  @Deprecated
  def unescapeKey(key : String) =
    key.replaceAll("u002e", "\\.") // .replaceAll("\\u0024", "\\$").replaceAll("\\\\", "\\")

  def jsonsToCsv(
    items: Traversable[JsObject],
    delimiter: String = ",",
    eol: String = "\n",
    explicitFieldNames: Seq[String] = Nil,
    replacements: Traversable[(String, String)] = Nil
  ) = {
    val sb = new StringBuilder(10000)

    if (items.nonEmpty || explicitFieldNames.nonEmpty) {

      // if field names are not explicitly provided use the fields of the first json
      val headerFieldNames = explicitFieldNames match {
        case Nil => items.head.fields.map(_._1)
        case _ => explicitFieldNames
      }

      // create a header
      def headerFieldName(fieldName: String) = unescapeKey(replaceAll(replacements)(fieldName))
      val header = headerFieldNames.map(headerFieldName).mkString(delimiter)
      sb.append(header + eol)

      // transform each json to a delimited string and add to a buffer
      items.foreach { item =>
        val row = jsonToDelimitedString(item, headerFieldNames, delimiter, replacements)
        sb.append(row + eol)
      }
    }

    sb.toString
  }

  def jsonToDelimitedString(
    json: JsObject,
    fieldNames: Traversable[String],
    delimiter: String = ",",
    replacements: Traversable[(String, String)] = Nil
  ): String = {
    val replaceAllAux = replaceAll(replacements)_
    val itemFieldNameValueMap = json.fields.toMap

    fieldNames.map { fieldName =>
      itemFieldNameValueMap.get(fieldName).fold("") { jsValue =>
        jsValue match {
          case JsNull => ""
          case _: JsString => replaceAllAux(jsValue.as[String])
          case _ => jsValue.toString()
        }
      }
    }.mkString(delimiter)
  }

  def traverse(json: JsObject, path: String): Seq[JsValue] = {
    // helper function to extract JS values from an JS object
    def extractJsValues(jsObject: JsObject, fieldName: String) =
      (jsObject \ fieldName).toOption.map ( jsValue =>
        jsValue match {
          case x: JsArray => x.value
          case _ => Seq(jsValue)
        }
      )

    path.split('.').foldLeft(Seq(json: JsValue)) {
      case (jsons: Seq[JsValue], fieldName) =>
        jsons.map { json =>
          json match {
            case x: JsObject => extractJsValues(x, fieldName)
            case _ => None
          }
        }.flatten.flatten
    }
  }

  def flatten(
    json: JsObject,
    delimiter: String = ".",
    excludedFieldNames: Set[String] = Set(),
    prefix: Option[String] = None
  ): JsObject =
    json.fields.foldLeft(Json.obj()) {
      case (acc, (fieldName, v)) =>
        val newPrefix = prefix.map(prefix => s"$prefix$delimiter$fieldName").getOrElse(fieldName)
        if (excludedFieldNames.contains(fieldName)) {
          acc + (newPrefix -> v)
        } else {
          v match {
            case jsObject: JsObject => acc.deepMerge(flatten(jsObject, delimiter, excludedFieldNames, Some(newPrefix)))
            case _ => acc + (newPrefix -> v)
          }
        }
    }

  def deflatten(
    json: JsObject,
    delimiter: String = "."
  ): JsObject = {

    // helper function to extract prefix and non-prefix fields
    def deflattenFields(
      fields: Seq[(String, JsValue)]
    ): Seq[(String, JsValue)] = {
      val prefixNonPrefixFields = fields.map { case (fieldName, jsValue) =>
        val prefixRest = fieldName.split(Pattern.quote(delimiter), 2)
        if (prefixRest.length < 2)
          Right((fieldName, jsValue))
        else
          Left((prefixRest(0), (prefixRest(1), jsValue)))
      }

      val prefixFields = prefixNonPrefixFields.flatMap(_.left.toOption)
      val nonPrefixFields = prefixNonPrefixFields.flatMap(_.right.toOption)

      val deflattedPrefixFields = prefixFields.groupBy(_._1).map { case (prefix, fields) =>
        val newFields = deflattenFields(fields.map(_._2))
        (prefix, JsObject(newFields))
      }

      nonPrefixFields ++ deflattedPrefixFields
    }

    JsObject(deflattenFields(json.fields))
  }

  private def replaceAll(
    replacements: Traversable[(String, String)])(
    value : String
  ) =
    replacements.foldLeft(value) { case (string, (from , to)) => string.replaceAll(from, to) }

  def filterAndSort(items : Seq[JsObject], orderBy : String, filter : String, filterFieldName : String) = {
    val filteredItems = if (filter.isEmpty) {
      items
    } else {
      val f = (filter + ".*").r
      items.filter { item =>
        val v = (item \ filterFieldName)
        f.unapplySeq(v.asOpt[String].getOrElse(v.toString())).isDefined
      }
    }

    val orderByField = if (orderBy.startsWith("-")) orderBy.substring(1) else orderBy

    val sortedItems = filteredItems.sortBy { item =>
      val v = (item \ orderByField)
      v.asOpt[String].getOrElse(v.toString())
    }

    if (orderBy.startsWith("-"))
      sortedItems.reverse
    else
      sortedItems
  }

  /**
    * Find items in field exactly matching input value.
    *
    * @param items Input items.
    * @param value Value for matching.
    * @param filterFieldName Field to be queried for value.
    * @return Found items.
    */
  def findBy(items : Seq[JsObject], value : String, filterFieldName : String) =
    items.filter { item =>
      val v = (item \ filterFieldName)
      v.asOpt[String].getOrElse(v.toString()).equals(value)
    }

  /**
    * Retrieve all items of specified field.
    *
    * @param items Input items.
    * @param fieldName Field of interest.
    * @return Items in specified field.
    */
  def project(items : Traversable[JsObject], fieldName : String): Traversable[JsReadable] =
    items.map { item => (item \ fieldName) }

  def toString(value: JsReadable): Option[String] =
    value match {
      case JsNull => None
      case JsString(s) => Some(s)
      case JsNumber(n) => Some(n.toString)
      case JsDefined(json) => toString(json)
      case _: JsUndefined => None
      case _ => Some(value.toString)
    }

  /**
    * Count objects of specified field to which the filter applies.
    *
    * @param items Json input items.
    * @param filter Filter string.
    * @param filterFieldName Name of he fields to be filtered.
    * @return Number of items to which the filter applies.
    */
  def count(items : Seq[JsObject], filter : String, filterFieldName : String): Int = {
    val filteredItems = if (filter.isEmpty) {
      items
    } else {
      val f = (filter + ".*").r
      items.filter { item =>
        val v = (item \ filterFieldName)
        f.unapplySeq(v.asOpt[String].getOrElse(v.toString())).isDefined
      }
    }
    filteredItems.length
  }
}