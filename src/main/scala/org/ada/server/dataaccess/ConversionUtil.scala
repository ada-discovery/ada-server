package org.ada.server.dataaccess

import java.text.{ParsePosition, ParseException, SimpleDateFormat}
import java.util.Date

import scala.reflect.ClassTag

object ConversionUtil {

  private val minValidYear = 1000
  private val maxValidYear = (1900 + new Date().getYear) + 1000

  def toDouble = convert(_.toDouble)_

  def toInt = convert(_.toInt)_

  def toLong = convert(_.toLong)_

  def toFloat = convert(_.toFloat)_

  def toBoolean(includeNumbers: Boolean = true) = convert(toBooleanAux(includeNumbers))_

  def toDate(dateFormats: Traversable[String]) = convert(toDateAux(dateFormats))_

  def toDateFromMsString = convert(toDateFromMsStringAux)_

  private def toDateAux(dateFormats: Traversable[String])(text: String) = {
    val dates = dateFormats.map { format =>
      try {
        val dateFormat = new SimpleDateFormat(format)
//        dateFormat.setLenient(false)
        val parsePosition = new ParsePosition(0)
        val date = dateFormat.parse(text, parsePosition)
        if (parsePosition.getIndex == text.length) {
          val year1900 = date.getYear
          // we assume that a valid year is bounded
          if (year1900 > minValidYear - 1900 && year1900 < maxValidYear - 1900)
            Some(date)
          else
            None
        } else
          None
      } catch {
        case e: ParseException => None
      }
    }.flatten

    dates.headOption.getOrElse(
      throw typeExpectedException(text, classOf[Date])
    )
  }

  private def toDateFromMsStringAux(text: String) = {
    val ms = try {
      toLong(text)
    } catch {
      case e: AdaConversionException => typeExpectedException(text, classOf[Date])
    }

    toDateFromMs(ms)
  }

  def toDateFromMs(ms: Long) = {
    val date = new Date(ms)
    val year1900 = date.getYear
    // we assume that a valid year is bounded
    if (year1900 > minValidYear - 1900 && year1900 < maxValidYear - 1900)
      date
    else
      typeExpectedException(ms.toString, classOf[Date])
  }

  private def toBooleanAux(includeNumbers: Boolean)(text: String) = {
    try {
      text.toBoolean
    } catch {
      case e: IllegalArgumentException =>
        if (includeNumbers)
          text match {
            case "0" => false
            case "1" => true
            case _ => typeExpectedException(text, classOf[Boolean])
          }
        else
          typeExpectedException(text, classOf[Boolean])
    }
  }

  def isDouble = isConvertible(_.toDouble)_

  def isInt = isConvertible(_.toInt)_

  def isLong = isConvertible(_.toLong)_

  def isFloat = isConvertible(_.toFloat)_

  def isBoolean = isConvertible(toBooleanAux(true))_

  def isDate(dateFormats: Traversable[String]) = isConvertible(toDateAux(dateFormats))_

  private def convert[T](
    fun: String => T)(
    text: String)(
    implicit tag: ClassTag[T]
  ): T = try {
    fun(text.trim)
  } catch {
    case _: NumberFormatException => typeExpectedException(text, tag.runtimeClass)
    case _: IllegalArgumentException => typeExpectedException(text, tag.runtimeClass)
  }

  private def isConvertible[T](
    fun: String => T)(
    text: String)(
    implicit tag: ClassTag[T]
  ): Boolean = try {
    convert[T](fun)(text); true
  } catch {
    case t: AdaConversionException => false
  }

  def typeExpectedException(value: String, expectedType: Class[_]) =
    throw new AdaConversionException(s"String '$value' is not ${expectedType.getSimpleName}-convertible.")
}

class AdaConversionException(message: String) extends RuntimeException(message)