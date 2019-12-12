package org.ada.server.runnables.core

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import javax.inject.{Inject, Named}
import org.ada.server.dataaccess.ElasticJsonCrudRepoFactory
import org.ada.server.dataaccess.dataset.FieldRepoFactory
import org.incal.core.dataaccess.CrudRepoExtra.CrudInfixOps
import org.incal.access.elastic.{ElasticCrudRepoExtra, ElasticSetting, RefreshPolicy}
import org.incal.core.dataaccess.StreamSpec
import org.incal.core.runnables.InputFutureRunnableExt
import play.api.{Configuration, Logger}

import scala.concurrent.ExecutionContext.Implicits.global

class CopyElasticDataSet @Inject()(
  fieldRepoFactory: FieldRepoFactory,
  @Named("ElasticJsonCrudRepoFactory") elasticDataSetRepoFactory: ElasticJsonCrudRepoFactory,
  configuration: Configuration
) extends InputFutureRunnableExt[CopyElasticDataSetSpec] {

  private val logger = Logger

  private implicit val system = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  override def runAsFuture(input: CopyElasticDataSetSpec) = {
    val sourceIndexName = "data-" + input.sourceDataSetId
    val targetIndexName = "data-" + input.targetDataSetId
    val fieldRepo = fieldRepoFactory(input.fieldDataSetId)

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

      // create a source Elastic index accessor
      sourceElasticRepo = {
        logger.info(s"Creating a repo accessor for the source index '$sourceIndexName' with ${fieldNameAndTypes.size} fields.")
        elasticDataSetRepoFactory(sourceIndexName, sourceIndexName, fieldNameAndTypes, Some(setting), false)
      }

      // create a temp Elastic index accessor
      targetElasticRepo = elasticDataSetRepoFactory(targetIndexName, targetIndexName, fieldNameAndTypes, Some(setting), input.targetDataSetExcludeIdMapping)

      // the source stream
      sourceStream <- sourceElasticRepo.findAsStream()

      _ <- {
        logger.info(s"Saving the data from the source index to the target one: '$targetIndexName'.")
        targetElasticRepo.saveAsStream(sourceStream)
      }
      _ <- targetElasticRepo.flushOps
    } yield
      ()
  }
}

case class CopyElasticDataSetSpec(
  sourceDataSetId: String,
  targetDataSetId: String,
  targetDataSetExcludeIdMapping: Boolean,
  fieldDataSetId: String,
  refreshPolicy: RefreshPolicy.Value,
  streamSpec: StreamSpec,
  scrollBatchSize: Int
)