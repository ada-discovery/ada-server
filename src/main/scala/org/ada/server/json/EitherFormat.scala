package org.ada.server.json

import play.api.libs.json._

private class EitherFormat[L, R](
    implicit val leftFormat: Format[L], rightFormat: Format[R]
  ) extends Format[Either[L, R]] {

  override def reads(json: JsValue): JsResult[Either[L, R]] = {
    val left = leftFormat.reads(json)
    val right = rightFormat.reads(json)

    if (left.isSuccess) {
      left.map(Left(_))
    } else if (right.isSuccess) {
      right.map(Right(_))
    } else {
      JsError(s"Unable to read Either type from JSON $json")
    }
  }

  override def writes(o: Either[L, R]): JsValue =
    o match {
      case Left(value) => leftFormat.writes(value)
      case Right(value) => rightFormat.writes(value)
    }
}

object EitherFormat {
  implicit def apply[L: Format, R: Format]: Format[Either[L, R]] = new EitherFormat[L, R]
}