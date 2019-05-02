package org.ada.server.services

import org.ada.server.AdaException

class AdaRestException(message: String) extends AdaException(message)

case class AdaUnauthorizedAccessRestException(message: String) extends AdaRestException(message)

