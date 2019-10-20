package org.ada.server.akka

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, Sink, Source, Unzip, Zip, ZipN}
import akka.util.ByteString

import scala.collection.mutable
import scala.collection.mutable.Buffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

object AkkaStreamUtil {

  def countFlow[A](
    maxSubstreams: Int = Int.MaxValue
  ): Flow[A, (A, Int), NotUsed] =
    Flow[A]
      .groupBy(maxSubstreams, identity)
      .map { a => a -> 1}
      .reduce((l, r) ⇒
        (l._1, l._2 + r._2)
      ).mergeSubstreams

  def uniqueFlow[A](
    maxSubstreams: Int = Int.MaxValue
  ): Flow[A, A, NotUsed] =
    Flow[A]
      .groupBy(maxSubstreams, identity)
      .reduce((l, _) ⇒ l)
      .mergeSubstreams

  def groupCountFlowTuple[A, B](
    maxSubstreams: Int = Int.MaxValue
  ): Flow[(A, B), (A, Int), NotUsed] =
    Flow[(A, B)]
      .groupBy(maxSubstreams, _._1)
      .map { case (a, _) => a -> 1}
      .reduce((l, r) ⇒ (l._1, l._2 + r._2))
      .mergeSubstreams

  def groupFlow[A, B](
    maxSubstreams: Int = Int.MaxValue
  ): Flow[(A, B), (A, Seq[B]), NotUsed] =
    Flow[(A,B)]
      .groupBy(maxSubstreams, _._1)
      .map { case (a, b) => a -> Buffer(b)}
      .reduce((l, r) ⇒ (l._1, {l._2.appendAll(r._2); l._2}))
      .mergeSubstreams

  def zipSources[A, B](
    source1: Source[A, _],
    source2: Source[B, _]
  ): Source[(A, B), NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val zip = b.add(Zip[A, B]())

      // connect the graph
      source1 ~> zip.in0
      source2 ~> zip.in1

      // expose the port
      SourceShape(zip.out)
    })

  def zipNFlows[T, U](
    flows: Seq[Flow[T, U, NotUsed]])(
  ): Flow[T, Seq[U], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[T](flows.size))
      val zipper = b.add(ZipN[U](flows.size))
      val flowsB = flows.map(flow => b.add(flow))

      flowsB.zipWithIndex.foreach { case (flow, i) => bcast.out(i) ~> flow.in }
      flowsB.zipWithIndex.foreach { case (flow, i) => flow.out ~> zipper.in(i)}

      FlowShape(bcast.in, zipper.out)
    })

  def applyTupleFlows[A_IN, A_OUT, B_IN, B_OUT](
    flow1: Flow[A_IN, A_OUT, NotUsed],
    flow2: Flow[B_IN, B_OUT, NotUsed]
  ): Flow[(A_IN, B_IN), (A_OUT, B_OUT), NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements

      val flow1B = b.add(flow1)
      val flow2B = b.add(flow2)
      val zip = b.add(Zip[A_OUT, B_OUT]())
      val unzip = b.add(Unzip[A_IN, B_IN]())

      // connect the elements

      unzip.out0 ~> flow1B.in
      unzip.out1 ~> flow2B.in

      flow1B.out ~> zip.in0
      flow2B.out ~> zip.in1

      FlowShape(unzip.in, zip.out)
    })

  def unzipNFlowsAndApply[T, U](
    seqSize: Int)(
    flow: Flow[T, U, NotUsed]
  ): Flow[Seq[T], Seq[U], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val unzipper = b.add(UnzipN[T](seqSize))
      val zipper = b.add(ZipN[U](seqSize))

      (0 to seqSize - 1).foreach { i => unzipper.out(i) ~> flow ~> zipper.in(i) }

      FlowShape(unzipper.in, zipper.out)
    })

  // note that an Option/None initialization solution is needed to make the flow threadsafe and reusable
  def seqFlow[T]: Flow[T, Seq[T], NotUsed] =
    Flow[T].fold(None: Option[mutable.Builder[T, Vector[T]]]) { case (builder, value) =>
      val defBuilder = builder.getOrElse(Vector.newBuilder[T])
      Some(defBuilder += value)
    }.map(_.map(_.result).getOrElse(Nil))

  def headAndTail[T, Mat](
    source: Source[T, Mat]
  ): (Source[T, Mat], Source[T, Mat]) = {
    val splitFlow = source.prefixAndTail(1)

    val head = splitFlow.map(_._1.head)
    val tail = splitFlow.flatMapConcat(_._2)

    (head, tail)
  }

  def fileHeaderAndContentSource(
    fileName: String,
    eol: String = "\n",
    allowTruncation: Boolean = true)(
    implicit materializer: Materializer
  ): Future[(String, Source[String, _])] = {
    val inputSource = FileIO
      .fromPath(Paths.get(fileName))
      .via(Framing.delimiter(ByteString(eol), 1000000, allowTruncation)
      .map(_.utf8String))

    val (headerSource, contentSource) = headAndTail(inputSource)

    headerSource.runWith(Sink.head).map ( header =>
      (header, contentSource)
    )
  }

  // TODO: since Akka 2.5.x provided by Source.fromFutureSource... remove once the lib is upgraded
  def fromFutureSource[out, mat](futureSource: Future[Source[out, mat]]): Source[out, NotUsed] =
    Source.fromGraph(Source.fromFuture(futureSource).flatMapConcat(identity))
}

object AkkaTest extends App {
  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  val source = Source(List(0, 1, 2))

  val sumFlow = Flow[Int].fold(0)(_+_)
  val minFlow = Flow[Int].fold(Integer.MAX_VALUE)(Math.min)
  val maxFlow = Flow[Int].fold(Integer.MIN_VALUE)(Math.max)
  val combinedFlow = AkkaStreamUtil.zipNFlows(Seq(sumFlow, minFlow, maxFlow))

  val resultsFuture = source.via(combinedFlow).runWith(Sink.head)
  val results = Await.result(resultsFuture, 1 minute)

  println(results.mkString("\n"))
}