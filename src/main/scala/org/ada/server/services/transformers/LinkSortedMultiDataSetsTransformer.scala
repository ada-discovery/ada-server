package org.ada.server.services.transformers

import akka.stream.scaladsl.{Flow, Source}
import org.ada.server.AdaException
import org.ada.server.field.FieldUtil
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps, NamedFieldType, fieldTypeOrdering}
import org.ada.server.models.DataSetFormattersAndIds.{FieldIdentity, JsObjectIdentity}
import org.ada.server.models.datatrans.{LinkMultiDataSetsTransformation, LinkSortedMultiDataSetsTransformation}
import org.incal.core.dataaccess.Criterion._
import org.incal.core.dataaccess.{AscSort, EqualsNullCriterion, NotEqualsNullCriterion}
import org.incal.core.util.crossProduct
import play.api.libs.json.{JsObject, Json}
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat
import org.incal.core.util.GroupMapList

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.math.Ordering
import scala.math.Ordering.Boolean

class LinkSortedMultiDataSetsTransformer
  extends AbstractDataSetTransformer[LinkSortedMultiDataSetsTransformation]
    with LinkDataSetsHelper[LinkSortedMultiDataSetsTransformation] {

  override protected def execInternal(
    spec: LinkSortedMultiDataSetsTransformation
  ) = {
    if (spec.linkedDataSetSpecs.size < 2)
      throw new AdaException(s"LinkSortedMultiDataSetsTransformer expects at least two data sets but got ${spec.linkedDataSetSpecs.size}.")

    for {
      // prepare data set infos with initialized accessors and loaded fields
      dataSetInfos <- Future.sequence(spec.linkedDataSetSpecs.map(createDataSetInfo))

      // split into the left and right sides
      leftDataSetInfo = dataSetInfos.head
      rightDataSetInfos = dataSetInfos.tail

      // check if the link fields are of the same type
      _ = {
        val linkFields = dataSetInfos.map { dataSetInfo =>
          val nameFieldMap = dataSetInfo.fields.map { field => (field.name, field) }.toMap

          dataSetInfo.linkFieldNames.map { linkFieldName =>
            nameFieldMap.get(linkFieldName).getOrElse(
              throw new AdaException(s"Link field '${linkFieldName}' not found in the data set ${dataSetInfo.dsa.dataSetId}.")
            )
          }
        }

        val notMatchedFields = linkFields.transpose.find { fields =>
          fields.tail.exists(f => !FieldUtil.areFieldTypesEqual(fields.head)(f))
        }

        if (notMatchedFields.isDefined)
          throw new AdaException(s"Link fields '${notMatchedFields.get.map(_.name).mkString(", ")}' are not of the same type.")
      }

      // left stream
      leftSource <- createLinkDataStream(leftDataSetInfo, 0)

      // right streams
      rightSources <- Future.sequence(
        rightDataSetInfos.zipWithIndex.map { case (dataInfo, index) =>
          createLinkDataStreamStripped(dataInfo, index + 1, spec.addDataSetIdToRightFieldNames)
        }
      )

      mergedStream = {
        // left link fields' orderings
        val leftLinkOrdering = linkOrdering(leftDataSetInfo)

        implicit val linkDataOrdering = new LinkDataOrdering(leftLinkOrdering)
        val mergedSource = (Seq(leftSource) ++ rightSources).reduceLeft(_.mergeSorted(_))

        // aux function to compare if source links are equal (and if the current one is defined)
        // this is a sign of transitioning from one link group to another
        def sourceLinksNotEq(
          curSourceLink: Option[Seq[Any]],
          nextSourceLink: Option[Seq[Any]]
        ) =
          curSourceLink.isDefined && (nextSourceLink.isEmpty || !leftLinkOrdering.equiv(curSourceLink.get, nextSourceLink.get))

        // use the merged sorted source/stream, buffer the entries with the same link (lead by the source 0 - left),
        // checks the link transitions (cur -> next), and finally link the data (sharing the same link)
        mergedSource
          .via(bufferFlow(leftLinkOrdering))
          .concat(Source(List(BufferAux(None, Nil))))
          .sliding(2,1)
          .collect { case els if sourceLinksNotEq(els(0).source1Link, els(1).source1Link) => els(0).buffer }
          .mapConcat { buffer =>
            val sourceIdGroupedData = buffer.map { case LinkData(_, sourceId, data) => (sourceId, data)}.toGroupMap
            link(rightDataSetInfos, spec.addDataSetIdToRightFieldNames)(sourceIdGroupedData.map(_._2))
          }
      }

      // create all possible streams with null link columns
      // note that because OR is currently not supported we need to do it exhaustively with AND
      nullStreams <- {
        val flags = binFlagsAscending(leftDataSetInfo.linkFieldNames.size).tail

        Future.sequence(
          flags.map(createEqNullStream(leftDataSetInfo))
        )
      }

      // collect all the relevant fields from all the input data sets
      newFields = createResultFields(leftDataSetInfo, rightDataSetInfos, spec.addDataSetIdToRightFieldNames)
    } yield {
      val finalStream = (Seq(mergedStream) ++ nullStreams).reduceLeft(_.concat(_))
      (leftDataSetInfo.dsa, newFields, finalStream, false)
    }
  }

  // aux function that creates a data stream for a given dsa
  private def createLinkDataStream(
    info: LinkedDataSetInfo,
    sourceId: Int
  ): Future[Source[LinkData, _]] = {
    if (info.fieldNames.nonEmpty)
      logger.info(s"Creating a stream for these fields ${info.fieldNames.mkString(",")}.")
    else
      logger.info(s"Creating a stream for all available fields.")

    val criteria = info.linkFieldNames.map(NotEqualsNullCriterion(_))

    for {
      source <- info.dsa.dataSetRepo.findAsStream(
        criteria = criteria,
        sort = info.linkFieldNames.map(AscSort(_)),
        projection = info.fieldNames
      )
    } yield {
      source.map { json =>
        val link = json.toValues(info.linkFieldTypes).map(_.get)
        LinkData(link, sourceId, json)
      }
    }
  }

  private def createLinkDataStreamStripped(
    info: LinkedDataSetInfo,
    sourceId: Int,
    addDataSetIdToFieldNames: Boolean
  ): Future[Source[LinkData, _]] =
    for {
      linkDataEntries <- createLinkDataStream(info, sourceId)
    } yield {
      val dataSetIdPrefixToAdd = if (addDataSetIdToFieldNames) Some(info.dsa.dataSetId) else None
      val linkFieldNameSet = info.linkFieldNames.toSet

      linkDataEntries.map { case LinkData(link, sourceId, json) =>
        // strip json (and rename if necessary)
        val strippedJson = stripJson(linkFieldNameSet, dataSetIdPrefixToAdd)(json)
        LinkData(link, sourceId, strippedJson)
      }
    }

  private def createEqNullStream(
    info: LinkedDataSetInfo)(
    equalsNulFlags: Seq[Boolean]
  ): Future[Source[JsObject, _]] = {
    if(info.fieldNames.nonEmpty)
      logger.info(s"Creating a stream for these fields ${info.fieldNames.mkString(",")}.")
    else
      logger.info(s"Creating a stream for all available fields.")

    val criteria = info.linkFieldNames.zip(equalsNulFlags).map { case (fieldName, eqNull) =>
      if (eqNull)
        EqualsNullCriterion(fieldName)
      else
        NotEqualsNullCriterion(fieldName)
    }

    for {
      source <- info.dsa.dataSetRepo.findAsStream(
        criteria = criteria,
        projection = info.fieldNames
      )
    } yield
      source
  }

  private def link(
    rightDataSetInfos: Seq[LinkedDataSetInfo],
    addDataSetIdToRightFieldNames: Boolean)(
    jsons: Traversable[Traversable[JsObject]]
  ): List[JsObject] = {
    // perform a cross-product of the json groups
    val jsonsCrossed = crossProduct(jsons)

    jsonsCrossed.map { jsons =>
      val linkedJson: JsObject = jsons.reduceLeft {_ ++ _}
      linkedJson ++ Json.obj(JsObjectIdentity.name -> JsObjectIdentity.next)
    }.toList
  }

  private def bufferFlow(ordering: Ordering[Seq[Any]]) =
    Flow[LinkData].scan[BufferAux](BufferAux(None, Nil)) {
      case (BufferAux(lastLink, buffer), linkData) =>

        val (newLastLink, newBuffer) =
          if (lastLink.isDefined && ordering.equiv(lastLink.get, linkData.link))
            (Some(linkData.link), buffer :+ linkData)
          else if (linkData.sourceId == 0)
            (Some(linkData.link), Seq(linkData))
          else
            (None, Nil)

        BufferAux(newLastLink, newBuffer)
    }


  private def binFlagsAscending(num: Int): Seq[Seq[Boolean]] =
    if (num < 1)
      Seq(Nil)
    else
      binFlagsAscending(num - 1).flatMap { flags =>
        Seq(false +: flags, true +: flags)
      }

  //////////////////////
  // Aux Data Holders //
  //////////////////////

  case class LinkData(
    link: Seq[Any],
    sourceId: Int,
    data: JsObject
  )

  case class BufferAux(
    source1Link: Option[Seq[Any]],
    buffer: Seq[LinkData]
  )

  ////////////////
  // Orderings  //
  ////////////////

  // aux function to create orderings for the link fields
  private def linkOrdering(
    info: LinkedDataSetInfo
  ): Ordering[Seq[Any]] = {
    val orderings = info.linkFieldTypes.map { namedFieldType =>
      val fieldType = namedFieldType._2
      fieldTypeOrdering(fieldType.spec.fieldType).getOrElse(
        throw new AdaException(s"No ordering available for the field type ${fieldType.spec.fieldType}.")
      )
    }
    new SeqOrdering[Any](orderings)
  }

  class SeqOrdering[T](orderings: Seq[Ordering[T]]) extends Ordering[Seq[T]]  {
    def compare(x: Seq[T], y: Seq[T]): Int = {
      val xe = x.iterator
      val ye = y.iterator
      val ord = orderings.iterator

      while (xe.hasNext && ye.hasNext && ord.hasNext) {
        val res = ord.next().compare(xe.next(), ye.next())
        if (res != 0) return res
      }

      Boolean.compare(xe.hasNext, ye.hasNext)
    }
  }

  class LinkDataOrdering(ordering: Ordering[Seq[Any]]) extends Ordering[LinkData] {

    def compare(linkData1: LinkData, linkData2: LinkData): Int = {
      val linkResult = ordering.compare(linkData1.link, linkData2.link)

      if (linkResult != 0)
        linkResult
      else
        linkData1.sourceId.compare(linkData2.sourceId)
    }
  }

  // We cannot use option ordering because for some source types (as Mongo), Nones/nulls come first,
  // whereas for others (such as Elastic) come last... the unified sorting could be maybe enforced
  @Deprecated
  class OptionLastOrdering[T](ordering: Ordering[T]) extends Ordering[Option[T]] {
    def compare(x: Option[T], y: Option[T]) = (x, y) match {
      case (None, None)       => 0
      case (None, _)          => 1
      case (_, None)          => -1
      case (Some(x), Some(y)) => ordering.compare(x, y)
    }
  }
}