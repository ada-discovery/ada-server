package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s.source.Indexable
import com.sksamuel.elastic4s.{HitAs, RichGetResponse, RichSearchHit, RichSearchResponse}
import org.incal.access.elastic.ElasticSerializer
import org.incal.access.elastic.caseclass.HasDynamicConstructor
import play.api.libs.json.{Format, Json, Reads}

trait ElasticFormatSerializer[E] extends ElasticSerializer[E] with HasDynamicConstructor[E] {

  protected implicit val format: Format[E]
  protected implicit val manifest: Manifest[E]

  protected implicit val indexable = new Indexable[E] {
    def json(t: E) = Json.toJson(t).toString()
  }

  override protected def serializeGetResult(response: RichGetResponse): Option[E] = {
    val originalResponse = response.original
    if (originalResponse.isExists)
      Some(Json.parse(originalResponse.getSourceAsBytes).as[E])
    else
      None
  }

  private implicit def toHitAs[A: Reads] = new HitAs[A] {
    def as(hit: RichSearchHit) = Json.parse(hit.source).as[A] // TODO: this.source or result.sourceAsString
  }

  override protected def serializeSearchResult(
    response: RichSearchResponse
  ): Traversable[E] =
    response.as[E]

  override protected def serializeSearchHit(
    result: RichSearchHit
  ): E = result.as[E]

  override protected def serializeProjectionSearchResult(
    projection: Seq[String],
    result: Traversable[(String, Any)]
  ) = {
    val fieldNameValueMap = result.toMap

    val constructor =
      constructorOrException(
        projection,
        fieldNameValueMap.get(concreteClassFieldName).map(_.asInstanceOf[String])
      )

    constructor(fieldNameValueMap).get
  }

  override protected def serializeProjectionSearchHits(
    projection: Seq[String],
    results: Array[RichSearchHit]
  ): Traversable[E] =
    if (!projection.contains(concreteClassFieldName)) {
      val constructor = constructorOrException(projection)
      results.map { result =>
        val fieldValues = result.fieldsSeq.map(field => (field.name, field.getValue[Any]))
        constructor(fieldValues.toMap).get
      }
    } else
      // TODO: optimize me... we should group the results by a concrete class field name
      results.map( serializeProjectionSearchHit(projection, _) )
}
