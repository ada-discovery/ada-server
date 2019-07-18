package org.ada.server.dataaccess

import org.ada.server.dataaccess.mongo.MongoAsyncCrudExtraRepo
import org.ada.server.models._
import org.ada.server.models.dataimport.DataSetImport
import org.ada.server.models.datatrans.DataSetMetaTransformation
import org.ada.server.models.ml.clustering.Clustering
import play.api.libs.json.JsObject
import reactivemongo.bson.BSONObjectID
import org.incal.core.dataaccess._
import org.incal.spark_ml.models.classification.Classifier
import org.incal.spark_ml.models.clustering.Clustering
import org.incal.spark_ml.models.regression.Regressor
import org.incal.spark_ml.models.result._

object RepoTypes {
  type JsonReadonlyRepo = AsyncReadonlyRepo[JsObject, BSONObjectID]
  type JsonCrudRepo = AsyncCrudRepo[JsObject, BSONObjectID]

  type DictionaryRootRepo = MongoAsyncCrudExtraRepo[Dictionary, BSONObjectID]

  type FieldRepo = AsyncCrudRepo[Field, String]
  type CategoryRepo = AsyncCrudRepo[Category, BSONObjectID]
  type FilterRepo = AsyncCrudRepo[Filter, BSONObjectID]
  type DataViewRepo = AsyncCrudRepo[DataView, BSONObjectID]

  type ClassificationResultRepo = AsyncCrudRepo[ClassificationResult, BSONObjectID]
  type StandardClassificationResultRepo = AsyncCrudRepo[StandardClassificationResult, BSONObjectID]
  type TemporalClassificationResultRepo = AsyncCrudRepo[TemporalClassificationResult, BSONObjectID]

  type RegressionResultRepo = AsyncCrudRepo[RegressionResult, BSONObjectID]
  type StandardRegressionResultRepo = AsyncCrudRepo[StandardRegressionResult, BSONObjectID]
  type TemporalRegressionResultRepo = AsyncCrudRepo[TemporalRegressionResult, BSONObjectID]

  type DataSetMetaInfoRepo = AsyncCrudRepo[DataSetMetaInfo, BSONObjectID]
  type DataSpaceMetaInfoRepo = AsyncCrudRepo[DataSpaceMetaInfo, BSONObjectID]

  type DataSetSettingRepo = AsyncCrudRepo[DataSetSetting, BSONObjectID]

  type UserRepo = AsyncCrudRepo[User, BSONObjectID]

  type TranslationRepo = AsyncCrudRepo[Translation, BSONObjectID]

  type MessageRepo = AsyncStreamRepo[Message, BSONObjectID]

  type DataSetImportRepo = AsyncCrudRepo[DataSetImport, BSONObjectID]
  type DataSetTransformationRepo = AsyncCrudRepo[DataSetMetaTransformation, BSONObjectID]

  type ClassifierRepo = AsyncCrudRepo[Classifier, BSONObjectID]
  type RegressorRepo = AsyncCrudRepo[Regressor, BSONObjectID]
  type ClusteringRepo = AsyncCrudRepo[Clustering, BSONObjectID]

  type HtmlSnippetRepo = AsyncCrudRepo[HtmlSnippet, BSONObjectID]
}