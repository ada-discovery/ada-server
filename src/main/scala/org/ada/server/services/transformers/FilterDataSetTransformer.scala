package org.ada.server.services.transformers

import org.ada.server.models.datatrans.FilterDataSetTransformation
import  org.ada.server.field.FieldUtil.toDataSetCriteria

import scala.concurrent.ExecutionContext.Implicits.global

private class FilterDataSetTransformer extends AbstractDataSetTransformer[FilterDataSetTransformation] {

  private val saveViewsAndFilters = true

  override protected def execInternal(
    spec: FilterDataSetTransformation
  ) = {
    val sourceDsa = dsaSafe(spec.sourceDataSetId)

    for {
      // get a filter
      filter <- sourceDsa.filterRepo.get(spec.filterId)

      // check if the filter exists
      _ = require(
        filter.isDefined,
        s"Filter '${spec.filterId.stringify}' cannot be found."
      )

      // turn filter's conditions into criteria
      criteria <- toDataSetCriteria(sourceDsa.fieldRepo, filter.get.conditions)

      // all the fields
      fields <- sourceDsa.fieldRepo.find()

      // input data stream
      inputStream <- sourceDsa.dataSetRepo.findAsStream(criteria)
    } yield
      (sourceDsa, fields, inputStream, saveViewsAndFilters)
  }
}