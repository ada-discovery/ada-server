package org.ada.server.dataaccess.mongo

import org.incal.core.dataaccess._
import org.incal.core.dataaccess.Criterion.Infix
import org.ada.server.dataaccess._
import org.incal.core.Identity
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsObject, _}
import reactivemongo.play.json.commands.JSONAggregationFramework.{Push, PushField, SumValue}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

abstract class SubordinateObjectMongoAsyncCrudRepo[E: Format, ID: Format, ROOT_E: Format,  ROOT_ID: Format](
    listName: String,
    rootRepo: MongoAsyncCrudExtraRepo[ROOT_E, ROOT_ID])(
    implicit identity: Identity[E, ID], rootIdentity: Identity[ROOT_E, ROOT_ID]
  ) extends AsyncCrudRepo[E, ID] {

  protected def getDefaultRoot: ROOT_E

  protected def getRootObject: Future[Option[ROOT_E]]

  /**
    * Initialize if root object does not exist.
    *
    * @return true, if initialization was required.
    */
  def initIfNeeded: Future[Boolean] = synchronized {
    val responseFuture = getRootObject.flatMap(rootObject =>
      if (rootObject.isEmpty)
        rootRepo.save(getDefaultRoot).map(_ => true)
      else
        Future(false)
    )

    // init root id
    responseFuture.map { response =>
      rootId;
      response
    }
  }

  protected lazy val rootId: ROOT_ID = synchronized {
    val futureId = getRootObject.map(rootObject => rootIdentity.of(rootObject.get).get)
    Await.result(futureId, 20 minutes)
  }

  protected lazy val rootIdSelector = Json.obj(rootIdentity.name -> rootId)

  /**
    * Converts the given subordinateListName into Json format and calls updateCustom() to update/ save it in the repo.
    *
    * @see updateCustom()
    * @param entity to be updated/ saved
    * @return subordinateListName name as a confirmation of success.
    */
  override def save(entity: E): Future[ID] = {
    val modifier = Json.obj {
      "$push" -> Json.obj {
        listName -> Json.toJson(entity)
      }
    }

    rootRepo.updateCustom(rootIdSelector, modifier) map { _ =>
      identity.of(entity).get
    }
  }

  override def save(entities: Traversable[E]): Future[Traversable[ID]] = {
    val modifier = Json.obj {
      "$push" -> Json.obj {
        listName -> Json.obj {
          "$each" -> entities
        }
      }
    }

    rootRepo.updateCustom(rootIdSelector, modifier) map { _ =>
      entities.map(identity.of(_).get)
    }
  }

  /**
    * Update a single subordinate in repo.
    * The properties of the passed subordinate replace the properties of the subordinate in the repo.
    *
    * @param entity Subordinate to be updated. entity.name must match an existing ID.
    * @return Id as a confirmation of success.
    */
  override def update(entity: E): Future[ID] = {
    val id = identity.of(entity)
    val selector =
      rootIdSelector + ((listName + "." + identity.name) -> Json.toJson(id))

    val modifier = Json.obj {
      "$set" -> Json.obj {
        listName + ".$" -> Json.toJson(entity)
      }
    }
    rootRepo.updateCustom(selector, modifier) map { _ =>
      id.get
    }
  }

//  override def update(entities: Traversable[E]): Future[Traversable[ID]] = super.update(entities)

  /**
    * Delete single entry identified by its id (name).
    *
    * @param id Id of the subordinate to be deleted.
    * @return Nothing (Unit)
    */
  override def delete(id: ID): Future[Unit] = {
    val modifier = Json.obj {
      "$pull" -> Json.obj {
        listName -> Json.obj {
          identity.name -> id
        }
      }
    }
    rootRepo.updateCustom(rootIdSelector, modifier)
  }

  override def delete(ids: Traversable[ID]): Future[Unit] = {
    val modifier = Json.obj {
      "$pull" -> Json.obj {
        listName -> Json.obj {
          identity.name -> Json.obj(
            "$in" -> ids
          )
        }
      }
    }
    rootRepo.updateCustom(rootIdSelector, modifier)
  }

  /**
    * Deletes all subordinates in the dictionary.
    *
    * @see update()
    * @return Nothing (Unit)
    */
  override def deleteAll: Future[Unit] = {
    val modifier = Json.obj {
      "$set" -> Json.obj {
        listName -> List[E]()
      }
    }
    rootRepo.updateCustom(rootIdSelector, modifier)
  }

  /**
    * Counts all items in repo matching criteria.
    *
    * @param criteria Filtering criteria object. Use a JsObject to filter according to value of reference column. Use None for no filtering.
    * @return Number of matching elements.
    */
  override def count(criteria: Seq[Criterion[Any]]): Future[Int] = {
    val rootCriteria = Seq(rootIdentity.name #== rootId)
    val subCriteria = criteria.map(criterion => criterion.copyWithFieldName(listName + "." + criterion.fieldName))

    val referencedFieldNames = (Seq(rootIdentity.name, listName + "." + identity.name) ++ subCriteria.map(_.fieldName)).toSet.toSeq

    val result = rootRepo.findAggregate(
      rootCriteria = rootCriteria,
      subCriteria = subCriteria,
      sort = Nil,
      projection = Some(JsObject(referencedFieldNames.map(_ -> JsNumber(1)))),
      idGroup = Some(JsNull),
      groups = Some(Seq("count" -> SumValue(1))),
      unwindFieldName = Some(listName),
      limit = None,
      skip = None
    )

    result.map {
      _.headOption.map(head => (head \ "count").as[Int]).getOrElse(0)
    }
  }

  override def exists(id: ID): Future[Boolean] =
    count(Seq(identity.name #== id)).map(_ > 0)

  /**
    * Retrieve subordinate(s) from the repo.
    *
    * @param id Name of object.
    * @return subordinateListNames in the dictionary with exact name match.
    */
  override def get(id: ID): Future[Option[E]] =
    find(Seq(identity.name #== id), limit = Some(1)).map(_.headOption)

  /**
    * Find object matching the filtering criteria. subordinateListNames may be ordered and only a subset of them used.
    * Pagination options for page limit and page number are available to limit number of returned results.
    *
    * @param criteria Filtering criteria object. Use a String to filter according to value of reference column. Use None for no filtering.
    * @param sort Column used as reference for sorting. Leave at None to use default.
    * @param projection Defines which columns are supposed to be returned. Leave at None to use default.
    * @param limit Page limit. Use to define chunk sizes for pagination. Leave at None to use default.
    * @param skip The number of items to skip.
    * @return Traversable subordinateListNames for iteration.
    */
  override def find(
    criteria: Seq[Criterion[Any]] = Nil,
    sort: Seq[Sort] = Nil,
    projection: Traversable[String] = Nil,
    limit: Option[Int] = None,
    skip: Option[Int] = None
  ): Future[Traversable[E]] = {
    val page: Option[Int] = None

    val rootCriteria = Seq(rootIdentity.name #== rootId)
    val subCriteria = criteria.map(criterion => criterion.copyWithFieldName(listName + "." + criterion.fieldName))

    val fullSort =
      sort.map(
        _ match {
          case AscSort(fieldName) => AscSort(listName + "." + fieldName)
          case DescSort(fieldName) => DescSort(listName + "." + fieldName)
        }
      )

    val fullProjection = projection.map(listName + "." + _)

    // TODO: projection can not be passed here since subordinateListName JSON formatter expects ALL attributes to be returned.
    // It could be solved either by making all subordinateListName attributes optional (Option[..]) or introducing a special JSON formatter with default values for each attribute
    val result = rootRepo.findAggregate(
      rootCriteria = rootCriteria,
      subCriteria = subCriteria,
      sort = fullSort,
      projection = None, //fullProjection,
      idGroup = Some(JsNull),
      groups = Some(Seq(listName -> PushField(listName))), // Push(listName)
      unwindFieldName = Some(listName),
      limit = limit,
      skip = skip
    )

    result.map { result =>
      if (result.nonEmpty) {
        val jsonFields = (result.head \ listName).as[JsArray].value // .asInstanceOf[Stream[JsValue]].force.toList
        jsonFields.map(_.as[E])
      } else
        Seq[E]()
    }
  }

  override def flushOps = rootRepo.flushOps
}