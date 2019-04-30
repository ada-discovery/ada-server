package org.ada.server.akka

import akka.stream.{Attributes, UniformFanOutShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable

object UnzipN {
  def apply[A](n: Int): GraphStage[UniformFanOutShape[Seq[A], A]] = new UnzipN[A](n)
}

private final class UnzipN[A](n: Int) extends UnzipWithN[Seq[A], A](identity)(n) {
  //  override def initialAttributes = DefaultAttributes.unzipN
  override def toString = "UnzipN"
}

object UnzipWithN {
  def apply[A, O](unzipper: A ⇒ immutable.Seq[O])(n: Int): GraphStage[UniformFanOutShape[A, O]] = new UnzipWithN[A, O](unzipper)(n)
}

private class UnzipWithN[A, O](unzipper: A ⇒ Seq[O])(n: Int) extends GraphStage[UniformFanOutShape[A, O]] {

  //  override def initialAttributes = DefaultAttributes.unzipWithN

  override val shape = new UniformFanOutShape[A, O](n)

  private def outSeq = shape.outArray.toSeq
  private val in = shape.in

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var pending = 0
    // Without this field the completion signalling would take one extra pull
    var willShutDown = false

    val grabInlet = grab[A] _
    val pullInlet = pull[A] _

    private def pushAll(): Unit = {
      outSeq.zip(unzipper(grabInlet(in))).foreach((push[O](_, _)).tupled)
      if (willShutDown)
        completeStage()
      else
        pullInlet(in)
    }

    override def preStart() =
      pullInlet(in)

    setHandler(in, new InHandler {
      override def onPush() = {
        pending -= n
        if (pending == 0) pushAll()
      }

      override def onUpstreamFinish() = {
        if (!isAvailable(in)) completeStage()
        willShutDown = true
      }
    })

    outSeq.map(
      setHandler(_, new OutHandler {
        override def onPull() = {
          pending += 1
          if (pending == 0) pushAll()
        }
      })
    )
  }

  override def toString = "UnzipWithN"
}