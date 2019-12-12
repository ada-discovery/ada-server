package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import javax.inject.{Inject, Named}
import org.ada.server.dataaccess.ElasticJsonCrudRepoFactory
import org.incal.core.dataaccess.CrudRepoExtra.CrudInfixOps
import org.ada.server.dataaccess.dataset.FieldRepoFactory
import org.incal.access.elastic.{ElasticCrudRepoExtra, ElasticSetting, RefreshPolicy}
import org.incal.core.dataaccess.StreamSpec
import org.incal.core.runnables.InputFutureRunnableExt
import play.api.{Configuration, Logger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class ReindexElasticDataSetWithCopying @Inject()(
  fieldRepoFactory: FieldRepoFactory,
  @Named("ElasticJsonCrudRepoFactory") elasticDataSetRepoFactory: ElasticJsonCrudRepoFactory,
  configuration: Configuration
) extends InputFutureRunnableExt[ReindexElasticDataSetWithCopyingSpec] {

  private val logger = Logger

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  override def runAsFuture(input: ReindexElasticDataSetWithCopyingSpec) = {
    val indexName = "data-" + input.dataSetId
    val tempIndexName = indexName + "-temp_" + Random.nextInt(10000)
    val fieldRepo = fieldRepoFactory(input.dataSetId)

    val setting = ElasticSetting(
      saveRefresh = input.refreshPolicy,
      saveBulkRefresh = input.refreshPolicy,
      scrollBatchSize = input.scrollBatchSize,
      indexFieldsLimit = configuration.getInt("elastic.index.fields.limit").getOrElse(10000)
    )

    for {
      // get all the fields
      fields <- fieldRepo.find()
      fieldNameAndTypes = fields.map(field => (field.name, field.fieldTypeSpec)).toSeq

      // create an original Elastic index accessor
      originalElasticRepo = {
        logger.info(s"Creating a repo accessor for the index '$indexName' with ${fieldNameAndTypes.size} fields.")
        elasticDataSetRepoFactory(indexName, indexName, fieldNameAndTypes, Some(setting), false)
      }

      // create a temp Elastic index accessor
      tempElasticRepo = elasticDataSetRepoFactory(tempIndexName, tempIndexName, fieldNameAndTypes, Some(setting), false)

      // the original stream
      originalStream <- originalElasticRepo.findAsStream()

      _ <- {
        logger.info(s"Saving the data from the original index to the temp one: '$tempIndexName'.")
        tempElasticRepo.saveAsStream(originalStream)
      }
      _ <- tempElasticRepo.flushOps

      // delete the original index
      _ <- {
        logger.info(s"Deleting and recreating the original index (with a new mapping).")
        originalElasticRepo.deleteAll
      }

      // get a data stream from the temp index
      stream <- tempElasticRepo.findAsStream()

      // save the data stream to the temp index
      _ <- {
        logger.info(s"Saving the data from the temp index back to the original one.")
        originalElasticRepo.saveAsStream(stream, input.streamSpec)
      }
      _ <- originalElasticRepo.flushOps

      // delete the temp index
      _ <- {
        logger.info(s"Deleting the temp index '$tempIndexName'.")
        tempElasticRepo.asInstanceOf[ElasticCrudRepoExtra].deleteIndex
      }
    } yield
      ()
  }
}

case class ReindexElasticDataSetWithCopyingSpec(
  dataSetId: String,
  refreshPolicy: RefreshPolicy.Value,
  streamSpec: StreamSpec,
  scrollBatchSize: Int
)