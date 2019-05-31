package runnables

import javax.inject.Inject
import org.ada.server.AdaException
import org.incal.core.runnables.InputFutureRunnableExt
import org.ada.server.dataaccess.dataset.DataSetAccessorFactory
import scala.reflect.runtime.universe.TypeTag

abstract class DsaInputFutureRunnable[I](implicit override val typeTag: TypeTag[I]) extends InputFutureRunnableExt[I] {

  @Inject var dsaf: DataSetAccessorFactory = _

  protected def createDsa(dataSetId: String) = dsaf(dataSetId).getOrElse(
    throw new AdaException(s"Data set id ${dataSetId} not found.")
  )

  protected def createDataSetRepo(dataSetId: String) = createDsa(dataSetId).dataSetRepo
}