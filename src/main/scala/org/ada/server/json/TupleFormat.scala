package org.ada.server.json

import play.api.libs.json.{JsObject, _}

private class Tuple2Format[A, B](
    implicit val firstFormat: Format[A], secondFormat: Format[B]
  ) extends Tuple2Writes[A, B] with Format[(A, B)] {

  override def reads(json: JsValue): JsResult[(A, B)] =
    json match {
      case JsArray(seq) =>
        if (seq.size == 2) {
          val first = firstFormat.reads(seq(0))
          val second = secondFormat.reads(seq(1))

          if (first.isSuccess && second.isSuccess)
            JsSuccess((first.get, second.get))
          else
            JsError(s"Unable to read Tuple2 type from JSON array $json.")
        } else {
          JsError(s"Unable to read Tuple2 type from JSON array since its size is ${seq.size}.")
        }

      case _ => JsError("JSON array value expected for Tuple2 type.")
    }
}

private class Tuple2Writes[A, B](
    implicit val firstWrites: Writes[A], secondWrites: Writes[B]
  ) extends Writes[(A, B)] {

  override def writes(o: (A, B)): JsValue =
    JsArray(Seq(
      firstWrites.writes(o._1),
      secondWrites.writes(o._2)
    ))
}

private class Tuple3Format[A, B, C](
    implicit val firstFormat: Format[A], secondFormat: Format[B], thirdFormat: Format[C]
  ) extends Tuple3Writes[A, B, C] with Format[(A, B, C)] {

  override def reads(json: JsValue): JsResult[(A, B, C)] =
    json match {
      case JsArray(seq) =>
        if (seq.size == 3) {
          val first = firstFormat.reads(seq(0))
          val second = secondFormat.reads(seq(1))
          val third = thirdFormat.reads(seq(2))

          if (first.isSuccess && second.isSuccess && third.isSuccess)
            JsSuccess((first.get, second.get, third.get))
          else
            JsError(s"Unable to read Tuple3 type from JSON array $json.")
        } else {
          JsError(s"Unable to read Tuple3 type from JSON array since its size is ${seq.size}.")
        }

      case _ => JsError("JSON array value expected for Tuple3 type.")
    }
}

private class Tuple3Writes[A, B, C](
    implicit val firstWrites: Writes[A], secondWrites: Writes[B], thirdWrites: Writes[C]
  ) extends Writes[(A, B, C)] {

  override def writes(o: (A, B, C)): JsValue =
    JsArray(Seq(
      firstWrites.writes(o._1),
      secondWrites.writes(o._2),
      thirdWrites.writes(o._3)
    ))
}

object TupleFormat {
  implicit def apply[A: Format, B: Format]: Format[(A, B)] = new Tuple2Format[A, B]
  implicit def apply[A: Format, B: Format, C: Format]: Format[(A, B, C)] = new Tuple3Format[A, B, C]
}

object TupleWrites {
  implicit def apply[A: Writes, B: Writes]: Writes[(A, B)] = new Tuple2Writes[A, B]
  implicit def apply[A: Writes, B: Writes, C: Writes]: Writes[(A, B, C)] = new Tuple3Writes[A, B, C]
}