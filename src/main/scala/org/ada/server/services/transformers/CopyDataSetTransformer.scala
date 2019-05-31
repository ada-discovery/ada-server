package org.ada.server.services.transformers

import org.ada.server.models.datatrans.CopyDataSetTransformation

import scala.concurrent.ExecutionContext.Implicits.global

private class CopyDataSetTransformer extends AbstractDataSetTransformer[CopyDataSetTransformation] {

  override protected def execInternal(
    spec: CopyDataSetTransformation
  ) = {
    val sourceDsa = dsaf(spec.sourceDataSetId).get

    for {
      // all the fields
      fields <- sourceDsa.fieldRepo.find()

      // input data stream
      inputStream <- sourceDsa.dataSetRepo.findAsStream()

    } yield
      (sourceDsa, fields, Some(inputStream))
  }
}