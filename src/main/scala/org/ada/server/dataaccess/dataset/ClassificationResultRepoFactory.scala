package org.ada.server.dataaccess.dataset

import org.ada.server.dataaccess.RepoTypes.ClassificationResultRepo

trait ClassificationResultRepoFactory {
  def apply(dataSetId: String): ClassificationResultRepo
}
