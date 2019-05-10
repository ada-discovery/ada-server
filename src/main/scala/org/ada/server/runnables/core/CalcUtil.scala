package org.ada.server.runnables.core

import java.{util => ju}

import org.ada.server.dataaccess.RepoTypes.FieldRepo
import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import org.ada.server.models.{Field, FieldTypeId}
import org.incal.core.dataaccess.Criterion._
import org.incal.core.util.seqFutures

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object CalcUtil {

  private val numericCriterion = "fieldType" #-> Seq(FieldTypeId.Double, FieldTypeId.Integer, FieldTypeId.Date)

  def numericFields(
    fieldRepo: FieldRepo)(
    fieldsNum: Option[Int] = None,
    fieldNamesToExclude: Seq[String] = Nil
  ): Future[Traversable[Field]] =
    fieldsNum.map( featuresNum =>
      fieldRepo.find(Seq(numericCriterion), limit = Some(featuresNum))
    ).getOrElse {
      val exclusionCriterion = fieldNamesToExclude match {
        case Nil => None
        case _ => Some(FieldIdentity.name #!-> fieldNamesToExclude)
      }

      fieldRepo.find(Seq(Some(numericCriterion), exclusionCriterion).flatten)
    }

  object repeatWithTime {

    def apply[A](
      repetitions: Int)(
      f: => Future[A]
    ): Future[(A, Int)] = {
      assert(repetitions > 0, "Repetitions must be > 0.")
      val calcStart = new ju.Date
      seqFutures(1 to repetitions) { _ => f }.map { results =>
        val execTimeMs = new ju.Date().getTime - calcStart.getTime
        val execTimeSec = execTimeMs.toDouble / (1000 * repetitions)
        (results.head, execTimeSec.toInt)
      }
    }
  }

  object repeatWithTimeOptional {

    def apply[A](
      repetitions: Int)(
      f: => Future[A]
    ): Future[Option[(A, Int)]] =
      if (repetitions > 0)
        repeatWithTime(repetitions)(f).map(Some(_))
      else
        Future(None)
  }
}