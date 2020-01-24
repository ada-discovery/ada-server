package org.ada.server.services.transformers

import akka.stream.scaladsl.StreamConverters
import org.ada.server.AdaException
import org.ada.server.dataaccess.dataset.DataSetAccessor
import org.ada.server.field.FieldType
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps, NamedFieldType, fieldTypeOrdering}
import org.ada.server.models.DataSetFormattersAndIds.{FieldIdentity, JsObjectIdentity}
import org.ada.server.models.Field
import org.ada.server.models.datatrans.{LinkMultiDataSetsTransformation, LinkedDataSetSpec}
import org.incal.core.dataaccess.Criterion._
import org.incal.core.dataaccess.{AscSort, NotEqualsNullCriterion}
import org.incal.core.util.{GroupMapList, crossProduct}
import play.api.libs.json.{JsObject, Json}
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private class LinkMultiDataSetsOptimizedTransformer
  extends AbstractDataSetTransformer[LinkMultiDataSetsTransformation]
    with LinkDataSetsHelper[LinkMultiDataSetsTransformation] {

  override protected def execInternal(
    spec: LinkMultiDataSetsTransformation
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