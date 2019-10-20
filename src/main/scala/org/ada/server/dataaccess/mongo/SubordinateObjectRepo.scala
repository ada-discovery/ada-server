package org.ada.server.dataaccess.mongo

import org.ada.server.AdaException
import org.incal.core.dataaccess._
import org.incal.core.dataaccess.Criterion.Infix
import org.ada.server.dataaccess._
import org.incal.core.Identity
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsObject, _}
import reactivemongo.play.json.commands.JSONAggregationFramework.{Push, PushField, SumValue}

import scala.concurrent.Future

abstract class SubordinateObjectMongoAsyncCrudRepo[E: Format, ID: Format, ROOT_E: Format,  ROOT_ID: Format](
    listName: String,
    rootRepo: MongoAsyncCrudExtraRepo[ROOT_E, ROOT_ID])(
    implicit identity: Identity[E, ID], rootIdentity: Identity[ROOT_E, ROOT_ID]
  ) extends AsyncCrudRepo[E, ID] {

  protected val logger = Logger

  protected def getDefaultRoot: ROOT_E

  protected var rootId: Option[Future[ROOT_ID]] = None

  protected def getRootObject: Future[Option[ROOT_E]]

  private def getRootIdSafe: Future[ROOT_ID] =
    rootId.getOrElse(synchronized { // use double checking here
      rootId.getOrElse(synchronized {
        rootId = Some(initRootId)
        rootId.get
      })
    })

  /**
    * Initialize if the root object does not exist.
    */
  def initIfNeeded: Future[Unit] = getRootIdSafe.map(_ => ())

  private def initRootId =
    for {
      // retrieve the Mongo root object
      rootObject <- {
        logger.debug(s"Initializing a subordinate mongo repo '${listName}'...")
        getRootObject
      }

      // if not available save a default one
      _ <- if (rootObject.isEmpty)
        rootRepo.save(getDefaultRoot)
      else
        Future(())

      // load it again to determine its id
      persistedRootObject <- getRootObject
    } yield {
      val rootObject = persistedRootObject.getOrElse(throw new AdaException(s"No root object found for the subordinate mongo repo '${listName}'."))
      rootIdentity.of(rootObject).getOrElse(throw new AdaException(s"The root object for the subordinate mongo repo '${listName}' has no id."))
    }

  protected def rootIdSelector: Future[JsObject] = getRootIdSafe.map(id => Json.obj(rootIdentity.name -> id))
  protected def rootIdCriteria = getRootIdSafe.map(id => Seq(rootIdentity.name #== id))

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

    for {
      selector <- rootIdSelector
      _ <- rootRepo.updateCustom(selector, modifier)
    } yield
      identity.of(entity).get
  }

  override def save(entities: Traversable[E]): Future[Traversable[ID]] = {
    val modifier = Json.obj {
      "$push" -> Json.obj {
        listName -> Json.obj {
          "$each" -> entities
        }
      }
    }

    for {
      selector <- rootIdSelector
      _ <- rootRepo.updateCustom(selector, modifier)
    } yield
      entities.map(identity.of(_).get)
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
    val modifier = Json.obj {
      "$set" -> Json.obj {
        listName + ".$" -> Json.toJson(entity)
      }
    }

    for {
      idSelector <- rootIdSelector
      selector = idSelector + ((listName + "." + identity.name) -> Json.toJson(id))
      _ <- rootRepo.updateCustom(selector, modifier)
    } yield
      id.get
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

    for {
      selector <- rootIdSelector
      _ <- rootRepo.updateCustom(selector, modifier)
    } yield
      ()
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

    for {
      selector <- rootIdSelector
      _ <- rootRepo.updateCustom(selector, modifier)
    } yield
      ()
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

    for {
      selector <- rootIdSelector
      _ <- rootRepo.updateCustom(selector, modifier)
    } yield
      ()
  }

  /**
    * Counts all items in repo matching criteria.
    *
    * @param criteria Filtering criteria object. Use a JsObject to filter according to value of reference column. Use None for no filtering.
    * @return Number of matching elements.
    */
  override def count(criteria: Seq[Criterion[Any]]): Future[Int] = {
    val subCriteria = criteria.map(criterion => criterion.copyWithFieldName(listName + "." + criterion.fieldName))

    val referencedFieldNames = (Seq(rootIdentity.name, listName + "." + identity.name) ++ subCriteria.map(_.fieldName)).toSet.toSeq

    for {
      rootCriteria <- rootIdCriteria
      results <- rootRepo.findAggregate(
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
    } yield
      results.headOption.map(head => (head \ "count").as[Int]).getOrElse(0)
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

    val subCriteria = criteria.map(criterion => criterion.copyWithFieldName(listName + "." + criterion.fieldName))

    val fullSort =
      sort.map(
        _ match {
          case AscSort(fieldName) => AscSort(listName + "." + fieldName)
          case DescSort(fieldName) => DescSort(listName + "." + fieldName)
        }
      )

    val fullProjection = projection.map(listName + "." + _)

    for {
      rootCriteria <- rootIdCriteria

      // TODO: projection can not be passed here since subordinateListName JSON formatter expects ALL attributes to be returned.
      // It could be solved either by making all subordinateListName attributes optional (Option[..]) or introducing a special JSON formatter with default values for each attribute
      results <- rootRepo.findAggregate(
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
    } yield
      if (results.nonEmpty) {
        val jsonFields = (results.head \ listName).as[JsArray].value // .asInstanceOf[Stream[JsValue]].force.toList
        jsonFields.map(_.as[E])
      } else
        Seq[E]()
  }

  override def flushOps = rootRepo.flushOps
}