package org.ada.server.dataaccess.elastic.format

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.exts.Logging
import org.incal.access.elastic.ElasticSerializer
import org.incal.access.elastic.caseclass.HasDynamicConstructor
import play.api.libs.json.{Format, JsResultException, Json, Reads}

trait ElasticFormatSerializer[E] extends ElasticSerializer[E] with HasDynamicConstructor[E] {

  logging: Logging =>

  protected implicit val format: Format[E]
  protected implicit val manifest: Manifest[E]

  protected implicit val indexable = new Indexable[E] {
    def json(t: E) = Json.toJson(t).toString()
  }

  override protected def serializeGetResult(response: GetResponse): Option[E] =
    if (response.exists)
      Some(Json.parse(response.sourceAsBytes).as[E])
    else
      None

  private implicit def toHitAs[A: Reads] = new HitReader[A] {
    def read(hit: Hit) = try {
      Right(Json.parse(hit.sourceAsBytes).as[A]) //         // TODO: this.source or result.sourceAsString
    } catch {
      case e: JsResultException => Left(e)
    }
  }

  override protected def serializeSearchResult(
    response: SearchResponse
  ): Traversable[E] =
    response.safeTo[E].flatMap(_.right.toOption)

  override protected def serializeSearchHit(
    result: SearchHit
  ): E = result.safeTo[E] match {
    case Right(json) => json
    case Left(e) => throw e
  }

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
    results: Array[SearchHit]
  ): Traversable[E] =
    if (!projection.contains(concreteClassFieldName)) {
      val constructor = constructorOrException(projection)
      results.map { result =>
        constructor(result.fields).get
      }
    } else
      // TODO: optimize me... we should group the results by a concrete class field name
      results.map( serializeProjectionSearchHit(projection, _) )
}
