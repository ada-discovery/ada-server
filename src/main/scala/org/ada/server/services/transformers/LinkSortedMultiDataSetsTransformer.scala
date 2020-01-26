package org.ada.server.services.transformers

import akka.stream.scaladsl.{Sink, Source, StreamConverters}
import org.ada.server.AdaException
import org.ada.server.field.FieldUtil.{FieldOps, JsonFieldOps, NamedFieldType, fieldTypeOrdering}
import org.ada.server.models.datatrans.{LinkMultiDataSetsTransformation, LinkWithSingleFieldMultiDataSetsTransformation, LinkedDataSetSpec}
import org.incal.core.dataaccess.Criterion._
import org.incal.core.dataaccess.{AscSort, NotEqualsNullCriterion}
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.math.Ordering
import scala.math.Ordering.{Boolean, Option}

private class LinkWithSingleFieldMultiDataSetsTransformer
  extends AbstractDataSetTransformer[LinkMultiDataSetsTransformation]
    with LinkDataSetsHelper[LinkMultiDataSetsTransformation] {

  type LinkData = (
    Seq[Option[Any]],  // link
    Int,               // the source id (the left one is 1)
    JsObject           // data
  )

  override protected def execInternal(
    spec: LinkMultiDataSetsTransformation
  ) = {
    // aux function that creates a data stream for a given dsa
    def createStream(
      info: LinkedDataSetInfo,
      sourceId: Int
    ): Future[Source[LinkData, _]] = {
      if(info.fieldNames.nonEmpty)
        logger.info(s"Creating a stream for these fields ${info.fieldNames.mkString(",")}.")
      else
        logger.info(s"Creating a stream for all available fields.")

      for {
        source <- info.dsa.dataSetRepo.findAsStream(
          sort = info.linkFieldNames.map(AscSort(_)),
          projection = info.fieldNames
        )
      } yield {
        source.map { json =>
          val link = json.toValues(info.linkFieldTypes)
          (link, sourceId, json)
        }
      }
    }

    // aux function to create orderings for the link fields
    def linkOrderings(
      info: LinkedDataSetInfo
    ): Ordering[LinkData] = {
      val orderings = info.linkFieldTypes.map { namedFieldType =>
        val fieldType = namedFieldType._2
        val ordering = fieldTypeOrdering(fieldType.spec.fieldType).getOrElse(
          throw new AdaException(s"No ordering available for the field type ${fieldType.spec.fieldType}.")
        )

        Option(ordering)
      }
      new LinkDataOrdering(orderings)
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
      leftStream <- createStream(leftDataSetInfo, 1)

      // right streams
      rightSources <- Future.sequence(rightDataSetInfos.zipWithIndex.map((createStream(_, _)).tupled))

      _ <- {
        implicit val ordering = leftLinkOrderings

        leftStream.mergeSorted(rightSources.head).runWith(Sink.foreach { case (link, id, json) =>
          println(link + " -> " + id)
        })
      }

      newFields <- leftDataSetInfo.dsa.fieldRepo.find()
    } yield
      (leftDataSetInfo.dsa, newFields, Source(Nil), false)
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

  class LinkDataOrdering(orderings: Seq[Ordering[Option[Any]]]) extends Ordering[LinkData] {
    private val linkOrdering = new SeqOrdering(orderings)

    def compare(linkData1: LinkData, linkData2: LinkData): Int = {
      val linkResult = linkOrdering.compare(linkData1._1, linkData2._1)

      if (linkResult != 0)
        return linkResult

      linkData1._2.compare(linkData2._2)
    }
  }
}