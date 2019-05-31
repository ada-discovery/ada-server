package org.ada.server.runnables.core

import javax.inject.Inject

import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import play.api.Logger
import org.incal.core.runnables.InputFutureRunnableExt

import scala.reflect.runtime.universe.typeOf
import scala.concurrent.ExecutionContext.Implicits.global

class RemoveUnreferencedFilters @Inject() (dsaf: DataSetAccessorFactory) extends InputFutureRunnableExt[RemoveUnreferencedFiltersSpec] {

  private val logger = Logger

  override def runAsFuture(
    input: RemoveUnreferencedFiltersSpec
  ) = {
    val dsa = dsaf(input.dataSetId).get

    for {
      // get all the views for a given data set
      views <- dsa.dataViewRepo.find()

      // get all the classification results for a given data set
      classificationResults <- dsa.classificationResultRepo.find()

      // get all the regression results for a given data set
      regressionResults <- dsa.regressionResultRepo.find()

      // get all the filters for a given data set
      allFilters <- dsa.filterRepo.find()

      // remove unreferenced filters
      _ <- {
        val refFilterIds1 = views.flatMap(_.filterOrIds.collect{ case Right(filterId) => filterId }).toSet
        val refFilterIds2 = classificationResults.flatMap(result => Seq(result.filterId, result.ioSpec.replicationFilterId).flatten).toSet
        val refFilterIds3 = regressionResults.flatMap(result => Seq(result.filterId, result.ioSpec.replicationFilterId).flatten).toSet

        val refFilterIds = refFilterIds1 ++ refFilterIds2 ++ refFilterIds3

        val allFilterIds = allFilters.flatMap(_._id).toSet

        val unreferencedFilterIds = allFilterIds.filterNot(refFilterIds.contains(_))

        logger.info(s"Removing ${unreferencedFilterIds.size} unreferenced filters for the data set ${input.dataSetId}.")

        dsa.filterRepo.delete(unreferencedFilterIds)
      }
    } yield
      ()
  }
}

case class RemoveUnreferencedFiltersSpec(
  dataSetId: String
)
