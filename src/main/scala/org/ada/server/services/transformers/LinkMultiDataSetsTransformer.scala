package org.ada.server.services.transformers

import akka.stream.scaladsl.StreamConverters
import org.ada.server.AdaException
import org.ada.server.dataaccess.dataset.DataSetAccessor
import org.ada.server.field.FieldType
import org.ada.server.field.FieldUtil.{JsonFieldOps, FieldOps, fieldTypeOrdering, NamedFieldType}
import org.ada.server.models.DataSetFormattersAndIds.{FieldIdentity, JsObjectIdentity}
import org.ada.server.models.Field
import org.ada.server.models.datatrans.{LinkMultiDataSetsTransformation, LinkedDataSetSpec}
import org.incal.core.dataaccess.AscSort
import org.incal.core.util.crossProduct
import org.incal.core.dataaccess.Criterion._
import play.api.libs.json.{JsObject, Json}
import org.incal.core.util.GroupMapList
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
import reactivemongo.bson.BSONObjectID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class LinkMultiDataSetsTransformer extends AbstractDataSetTransformer[LinkMultiDataSetsTransformation] {

  override protected def execInternal(
    spec: LinkMultiDataSetsTransformation
  ) = {

    if (spec.linkedDataSetSpecs.size < 2)
      throw new AdaException(s"LinkMultiDataSetsTransformer expects at least two data sets but got ${spec.linkedDataSetSpecs.size}.")


    for {
      // prepare data set infos with initialized accessors and load fields
      dataSetInfos <- Future.sequence(spec.linkedDataSetSpecs.map(createDataSetInfo))

      // split into the left and right sides
      leftDataSetInfo = dataSetInfos.head
      rightDataSetInfos = dataSetInfos.tail

      // create linked json data for the right data sets stored as a list of maps
      linkRightJsonsMaps <- Future.sequence(
        rightDataSetInfos.map { rightDataSetInfo =>
          linkJsonsMap(rightDataSetInfo, spec.addDataSetIdToRightFieldNames)
        }
      )

      // collect all the fields for the new data set
      newFields = {
        val rightFieldsWoLink = rightDataSetInfos.flatMap { rightDataSetInfo =>

          val linkFieldNameSet = rightDataSetInfo.linkFieldNames.toSet
          val fieldsWoLink = rightDataSetInfo.fields.filterNot {field => linkFieldNameSet.contains(field.name)}.toSeq

          if (spec.addDataSetIdToRightFieldNames)
            fieldsWoLink.map { field =>
              val newFieldName = rightDataSetInfo.dsa.dataSetId.replace('.', '_') + "-" + field.name
              field.copy(name = newFieldName)
            }
          else
            fieldsWoLink
        }

        leftDataSetInfo.fields ++ rightFieldsWoLink
      }

      // create an input stream for the left data set
      originalStream <- leftDataSetInfo.dsa.dataSetRepo.findAsStream(projection = leftDataSetInfo.fieldNames)

      // add linked data to the stream
      finalStream = originalStream.mapConcat(link(leftDataSetInfo, linkRightJsonsMaps))
    } yield {
      // if the left data set has all the fields preserved (i.e., no explicitly provided ones) then we save its views and filters
      val saveViewsAndFilters = spec.linkedDataSetSpecs.head.explicitFieldNamesToKeep.isEmpty

      (leftDataSetInfo.dsa, newFields, finalStream, saveViewsAndFilters)
    }
  }

  private def createDataSetInfo(
    spec: LinkedDataSetSpec
  ): Future[LinkedDataSetInfo] = {
    // data set accessor
    val dsa = dsaf(spec.dataSetId).getOrElse(throw new AdaException(s"Data Set ${spec.dataSetId} not found."))

    // determine which fields to load (depending on whether preserve field names are specified)
    val fieldNamesToLoad = spec.explicitFieldNamesToKeep match {
      case Nil => Nil
      case _ => (spec.explicitFieldNamesToKeep ++ spec.linkFieldNames).toSet
    }

    for {
      // load fields
      fields <- fieldNamesToLoad match {
        case Nil => dsa.fieldRepo.find()
        case _ => dsa.fieldRepo.find(Seq(FieldIdentity.name #-> fieldNamesToLoad.toSeq))
      }
    } yield {
      // collect field types (in order) for the link
      val nameFieldMap = fields.map(field => (field.name, field)).toMap

      val linkFieldTypes = spec.linkFieldNames.map { fieldName =>
        nameFieldMap.get(fieldName).map(_.toNamedTypeAny).getOrElse(
          throw new AdaException(s"Link field $fieldName not found.")
        )
      }

      LinkedDataSetInfo(dsa, spec.linkFieldNames, fields, linkFieldTypes)
    }
  }

  case class LinkedDataSetInfo(
    dsa: DataSetAccessor,
    linkFieldNames: Seq[String],
    fields: Traversable[Field],
    linkFieldTypes: Seq[NamedFieldType[Any]]
  ) {
    val fieldNames = fields.map(_.name)
  }

  private def link(
    leftDataSetInfo: LinkedDataSetInfo,
    linkRightJsonsMaps: Seq[Map[Seq[String], Traversable[JsObject]]])(
    json: JsObject
  ): List[JsObject] = {
    val link = json.toDisplayStrings(leftDataSetInfo.linkFieldTypes)
    val jsonId = (json \ JsObjectIdentity.name).asOpt[BSONObjectID]

    val rightJsonsCrossed = crossProduct(linkRightJsonsMaps.flatMap(_.get(link)))

    if (rightJsonsCrossed.isEmpty) {
      List(json)
    } else {
      rightJsonsCrossed.map { rightJsons =>
        val rightJson: JsObject = rightJsons.foldLeft(Json.obj()) {_ ++ _}
        val id = if (rightJsonsCrossed.size > 1 || jsonId.isEmpty) JsObjectIdentity.next else jsonId.get

        json ++ rightJson ++ Json.obj(JsObjectIdentity.name -> id)
      }.toList
    }
  }

  // a helper function to load the jsons for a given data set and create a link -> jsons map
  private def linkJsonsMap(
    dataSetInfo: LinkedDataSetInfo,
    addDataSetIdToRightFieldNames: Boolean
  ): Future[Map[Seq[String], Traversable[JsObject]]] =
    for {
      jsons <- dataSetInfo.dsa.dataSetRepo.find(projection = dataSetInfo.fieldNames)
    } yield {
      val linkFieldNameSet = dataSetInfo.linkFieldNames.toSet
      jsons.map { json =>
        // create a link as a sequence of display strings
        val link = json.toDisplayStrings(dataSetInfo.linkFieldTypes)

        // remove the link fields from a json
        val strippedJson = json.fields.filterNot { case (fieldName, _) => linkFieldNameSet.contains(fieldName) }

        // rename if necessary
        val renamedJson = if (addDataSetIdToRightFieldNames) {
          strippedJson.map { case (fieldName, jsValue) =>
            val newFieldName = dataSetInfo.dsa.dataSetId.replace('.', '_') + "-" + fieldName
            (newFieldName, jsValue)
          }
        } else
          strippedJson

        (link, JsObject(renamedJson))
      }.toGroupMap
    }

  // TODO: alternative implementation with pointers and sorting
  private def linkImproved(
    spec: LinkMultiDataSetsTransformation)(
    implicit materializer: akka.stream.Materializer
  ) = {

    // aux function that creates a data stream for a given dsa
    def createStream(info: LinkedDataSetInfo) = {
      if(info.fieldNames.nonEmpty)
        logger.info(s"Creating a stream for these fields ${info.fieldNames.mkString(",")}.")
      else
        logger.info(s"Creating a stream for all available fields.")

      info.dsa.dataSetRepo.findAsStream(
        sort = info.linkFieldNames.map(AscSort(_)),
        projection = info.fieldNames
      )
    }

    // aux function to create orderings for the link fields
    def linkOrderings(
      info: LinkedDataSetInfo
    ): Seq[Ordering[Any]] =
      info.linkFieldTypes.map { namedFieldType =>
        val fieldType = namedFieldType._2
        fieldTypeOrdering(fieldType.spec.fieldType).getOrElse(
          throw new AdaException(s"No ordering available for the field type ${fieldType.spec.fieldType}.")
        )
      }

    for {
      // prepare data set infos with initialized accessors and load fields
      dataSetInfos <- Future.sequence(spec.linkedDataSetSpecs.map(createDataSetInfo))

      // split into the left and right sides
      leftDataSetInfo = dataSetInfos.head
      rightDataSetInfos = dataSetInfos.tail

//      // register the result data set (if not registered already)
//      linkedDsa <- dataSetService.registerDerivedDataSet(leftDataSetInfo.dsa, spec.resultDataSetSpec)

      // left link fields' orderings
      leftLinkOrderings = linkOrderings(leftDataSetInfo)

      // right link fields' orderings
      rightLinkOrderings = rightDataSetInfos.map(linkOrderings)

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
            val rightLink = rightJson.toValues(info.linkFieldTypes)

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

          val leftLink = leftJson.toValues(leftDataSetInfo.linkFieldTypes)



          leftJson
        }
        Future(())
      }
    } yield
      ()
  }
}