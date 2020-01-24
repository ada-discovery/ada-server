package org.ada.server.services.transformers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Sink

object MergeSortedProcessing extends App {

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  private val sourceA = Source(List(0, 1, 1, 5, 7).map((_, 1)))
  private val sourceB = Source(List(2, 4, 6, 7, 8).map((_, 2)))
  private val sourceC = Source(List(1, 1, 1, 7, 20).map((_, 3)))

  val sources = Seq(sourceA, sourceB, sourceC)

  val mergedSource = sources.reduceLeft(_.mergeSorted(_))

  val bufferFlow = Flow[(Int, Int)].scan[BufferAux](BufferAux(None, Nil)) {
    case (BufferAux(lastValue, buffer), (value, sourceId)) =>

      val (newLastValue, newBuffer) =
        if (lastValue.isDefined && lastValue.get == value)
          (Some(value), buffer :+ (value, sourceId))
        else if (sourceId == 1)
          (Some(value), Seq((value, sourceId)))
        else
          (None, Nil)

      BufferAux(newLastValue, newBuffer)
  }

  mergedSource
    .via(bufferFlow)
    .concat(Source(List(BufferAux(None, Nil))))
    .sliding(2,1)
    .collect { case els if els(0).source1Value.isDefined && els(0).source1Value != els(1).source1Value => els(0) }
    .runWith(Sink.foreach(println))

  case class BufferAux(
    source1Value: Option[Int],
    buffer: Seq[(Int, Int)]
  )
}
