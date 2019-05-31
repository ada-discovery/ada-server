package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.google.inject.Inject
import org.ada.server.models.FieldTypeId
import org.apache.commons.lang3.StringEscapeUtils
import org.incal.core.runnables.{InputFutureRunnable, InputFutureRunnableExt}
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import org.ada.server.runnables.core.CalcUtil._
import org.ada.server.calc.CalculatorExecutors
import org.incal.core.dataaccess.Criterion._

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class CalcMatthewsBinaryClassCorrelations @Inject()(
    dsaf: DataSetAccessorFactory
  ) extends InputFutureRunnableExt[CalcMatthewsBinaryClassCorrelationsSpec] with CalculatorExecutors {

  private val logger = Logger

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()
  private val defaultDelimiter = ","

  override def runAsFuture(input: CalcMatthewsBinaryClassCorrelationsSpec) = {
    val delimiter = StringEscapeUtils.unescapeJava(input.delimiter.getOrElse(defaultDelimiter))
    val dsa = dsaf(input.dataSetId).get

    for {
      // get the boolean fields
      booleanFields <- dsa.fieldRepo.find(Seq("fieldType" #== FieldTypeId.Boolean))

      // sorted fields
      sortedFields = booleanFields.toSeq.sortBy(_.name)

      // calculate Matthews (binary class) correlations
      corrs <- matthewsBinaryClassCorrelationExec.execJsonRepoStreamed(
        Some(input.parallelism),
        Some(input.parallelism),
        true,
        sortedFields)(
        dsa.dataSetRepo, Nil
      )

    } yield {
      logger.info(s"Exporting the calculated correlations to ${input.exportFileName}.")

      FeatureMatrixIO.saveSquare(
        corrs,
        sortedFields.map(_.name),
        input.exportFileName,
        (value: Option[Double]) => value.map(_.toString).getOrElse(""),
        delimiter
      )
    }
  }
}

case class CalcMatthewsBinaryClassCorrelationsSpec(
  dataSetId: String,
  parallelism: Int,
  delimiter: Option[String],
  exportFileName: String
)