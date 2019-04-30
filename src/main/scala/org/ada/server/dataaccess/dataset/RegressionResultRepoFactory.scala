package org.ada.server.dataaccess.dataset

import org.ada.server.dataaccess.RepoTypes.RegressionResultRepo

trait RegressionResultRepoFactory {
  def apply(dataSetId: String): RegressionResultRepo
}
