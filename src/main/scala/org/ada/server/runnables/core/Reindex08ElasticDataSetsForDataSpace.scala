package org.ada.server.runnables.core

import javax.inject.{Inject, Named}
import org.ada.server.AdaException
import org.ada.server.dataaccess.{ElasticJsonCrudRepoFactory, StreamSpec}
import org.ada.server.dataaccess.RepoTypes.{DataSetSettingRepo, DataSpaceMetaInfoRepo}
import org.ada.server.dataaccess.elastic.format.ElasticIdRenameUtil
import org.ada.server.models.StorageType
import org.incal.access.elastic.{ElasticCrudRepoExtra, RefreshPolicy}
import org.incal.core.dataaccess.Criterion._
import org.incal.core.runnables.{FutureRunnable, InputFutureRunnableExt, RunnableHtmlOutput}
import reactivemongo.bson.BSONObjectID
import org.incal.core.util.seqFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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

      dataSetsToMigrate <- dataSetIdsToMigrate(dataSetIds.toSeq)

      _ <- seqFutures(dataSetsToMigrate) { dataSetId =>
        reindexElasticDataSet.runAsFuture(ReindexElasticDataSetSpec(
          dataSetId,
          input.refreshPolicy,
          input.streamSpec,
          input.scrollBatchSize
        ))
      }
    } yield {
      addParagraph(s"Migrated ${bold(dataSetsToMigrate.size.toString)} Elastic data sets (out of ${dataSetIds.size}) for which it was needed.")
      addOutput(reindexElasticDataSet.output.toString())
    }
}

case class ReindexAll08ElasticDataSetsSpec(
  refreshPolicy: RefreshPolicy.Value,
  streamSpec: StreamSpec,
  scrollBatchSize: Int
)

class Reindex08ElasticDataSetsForDataSpace @Inject()(
  dataSpaceMetaInfoRepo: DataSpaceMetaInfoRepo,
  reindexElasticDataSet: ReindexElasticDataSet
) extends InputFutureRunnableExt[ReindexElasticDataSetsForDataSpaceSpec]
  with List08ElasticDataSetsToMigrateHelper
  with RunnableHtmlOutput {

  override def runAsFuture(input: ReindexElasticDataSetsForDataSpaceSpec) =
    for {
      dataSpace <- dataSpaceMetaInfoRepo.get(input.dataSpaceId).map(_.getOrElse(throw new AdaException(s"Data space ${input.dataSpaceId.stringify} not found.")))
      dataSetIds = dataSpace.dataSetMetaInfos.map(_.id)

      dataSetsToMigrate <- dataSetIdsToMigrate(dataSetIds)

      _ <- seqFutures(dataSetsToMigrate) { dataSetId =>
        reindexElasticDataSet.runAsFuture(ReindexElasticDataSetSpec(
          dataSetId,
          input.refreshPolicy,
          input.streamSpec,
          input.scrollBatchSize
        ))
      }
    } yield {
      addParagraph(s"Migrated ${bold(dataSetsToMigrate.size.toString)} Elastic data sets (out of ${dataSetIds.size}) for which it was needed.")
      addOutput(reindexElasticDataSet.output.toString())
    }
}

case class ReindexElasticDataSetsForDataSpaceSpec(
  dataSpaceId: BSONObjectID,
  refreshPolicy: RefreshPolicy.Value,
  streamSpec: StreamSpec,
  scrollBatchSize: Int
)