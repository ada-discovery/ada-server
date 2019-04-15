package org.ada.server

class AdaException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}

case class AdaParseException(message: String, cause: Throwable) extends AdaException(message, cause) {
  def this(message: String) = this(message, null)
}
