package org.ada.server.util

import org.ada.server.models.Message
import play.api.{LoggerLike, Logger}
import org.ada.server.dataaccess.RepoTypes.MessageRepo

private class MessageLogger(_logger: LoggerLike, messageRepo: MessageRepo) extends LoggerLike {

  override val logger = _logger.underlyingLogger

  override def trace(message: => String) =
    withMessage(message)(super.trace(message))

  override def trace(message: => String, error: => Throwable) =
    withMessage(message)(super.trace(message, error))

  override def debug(message: => String) =
    withMessage(message)(super.debug(message))

  override def debug(message: => String, error: => Throwable) =
    withMessage(message)(super.debug(message, error))

  override def info(message: => String) =
    withMessage(message)(super.info(message))

  override def info(message: => String, error: => Throwable) =
    withMessage(message)(super.info(message, error))

  override def warn(message: => String) =
    withMessage(message)(super.warn(message))

  override def warn(message: => String, error: => Throwable) =
    withMessage(message)(super.warn(message, error))

  override def error(message: => String) =
    withMessage(message)(super.error(message))

  override def error(message: => String, error: => Throwable) =
    withMessage(message)(super.error(message, error))

  private def withMessage(message: String)(action: => Unit) = {
    action; messageRepo.save(Message(None, message))
  }
}

object MessageLogger {
  def apply(logger: LoggerLike, messageRepo: MessageRepo): LoggerLike = new MessageLogger(logger, messageRepo)
}