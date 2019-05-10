package runnables

import javax.inject.Inject

import org.ada.server.AdaException
import org.incal.core.runnables.InputFutureRunnable
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory

trait DsaInputFutureRunnable[I] extends InputFutureRunnable[I] {

  @Inject var dsaf: DataSetAccessorFactory = _

  protected def createDsa(dataSetId: String) = dsaf(dataSetId).getOrElse(
    throw new AdaException(s"Data set id ${dataSetId} not found.")
  )

  protected def createDataSetRepo(dataSetId: String) = createDsa(dataSetId).dataSetRepo
}