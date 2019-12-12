package org.ada.server.runnables.core

import javax.inject.Inject
import org.ada.server.dataaccess.RepoTypes.DataSpaceMetaInfoRepo
import org.incal.core.dataaccess.StreamSpec
import org.incal.access.elastic.RefreshPolicy
import org.incal.core.runnables.{InputFutureRunnableExt, RunnableHtmlOutput}
import org.incal.core.util.seqFutures
import scala.concurrent.ExecutionContext.Implicits.global

class ReindexAll08ElasticDataSets @Inject()(
  dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo,
  reindexElasticDataSet: ReindexElasticDataSet
) extends InputFutureRunnableExt[ReindexAll08ElasticDataSetsSpec]
  with List08ElasticDataSetsToMigrateHelper
  with RunnableHtmlOutput {

  override def runAsFuture(input: ReindexAll08ElasticDataSetsSpec) =
    for {
      dataSpaces <- dataSpaceMetaInfoRepo.find()
      dataSetIds = dataSpaces.flatMap(_.dataSetMetaInfos.map(_.id))

      dataSetsToMigrate <- dataSetIdsToMigrate(dataSetIds.toSeq, input.mappingsLimit)

      _ <- seqFutures(dataSetsToMigrate) { dataSetId =>
        reindexElasticDataSet.runAsFuture(ReindexElasticDataSetSpec(
          dataSetId,
          RefreshPolicy.None,
          StreamSpec(
            Some(input.saveBatchSize),
            Some(input.saveBatchSize),
            Some(1)
          ),
          input.scrollBatchSize
        ))
      }
    } yield {
      addParagraph(s"Migrated ${bold(dataSetsToMigrate.size.toString)} Elastic data sets (out of ${dataSetIds.size}) for which it was needed.")
      addOutput(reindexElasticDataSet.output.toString())
    }
}

case class ReindexAll08ElasticDataSetsSpec(
  saveBatchSize: Int,
  scrollBatchSize: Int,
  mappingsLimit: Option[Int]
)