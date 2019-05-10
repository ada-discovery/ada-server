package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.models.DataSetFormattersAndIds.FieldIdentity
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import org.incal.core.dataaccess.Criterion.Infix
import org.incal.core.util.seqFutures

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ChangeFieldLabels @Inject() (dsaf: DataSetAccessorFactory) extends InputFutureRunnable[ChangeFieldLabelsSpec] {

  override def runAsFuture(
    input: ChangeFieldLabelsSpec
  ) = {
    val dsa = dsaf(input.dataSetId).get

    val nameLabelMap = input.fieldNameLabels.grouped(2).toSeq.map(seq => (seq(0), seq(1))).toMap
    val names = nameLabelMap.map(_._1).toSeq

    for {
      fields <- dsa.fieldRepo.find(Seq(FieldIdentity.name #-> names))

      _ <- {
        val newLabelFields = fields.map { field =>
          val newLabel = nameLabelMap.get(field.name).get
          field.copy(label = Some(newLabel))
        }

        input.batchSize.map( batchSize =>
          seqFutures(newLabelFields.toSeq.grouped(batchSize))(dsa.fieldRepo.update)
        ).getOrElse(
          dsa.fieldRepo.update(newLabelFields)
        )
      }
    } yield
      ()
  }

  override def inputType = typeOf[ChangeFieldLabelsSpec]
}

case class ChangeFieldLabelsSpec(
  dataSetId: String,
  fieldNameLabels: Seq[String],
  batchSize: Option[Int]
)