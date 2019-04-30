package org.ada.server.dataaccess.dataset

import collection.mutable.{Map => MMap}
import scala.concurrent.{Future, Await}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._

// TODO: introduce a read-write lock
abstract class RefreshableCache[ID, T](eagerLoad: Boolean) {
  //  protected val lock: Lock
  protected val cache = {
    val map = MMap[ID, T]()
    if (eagerLoad)
      refresh(map)
    map
  }

  def apply(id: ID): Option[T] =
    cache.get(id) match {
      case Some(item) => Some(item)
      case None =>
        val future = createInstance(id).map { instance =>
          if (instance.isDefined)
            cache.put(id, instance.get)
          instance
        }
        Await.result(future, 10 minutes)
    }

  protected def cacheMissGet(id: ID): Future[Option[T]] = Future(None)

  private def refresh(map : MMap[ID, T]): Unit = this.synchronized {
    map.clear()
    val future =
      for {
        // collect all ids
        ids <- getAllIds

        // create all instances
        idInstances <- createInstances(ids)
      } yield
        idInstances.map { case (id, instance) =>
          map.put(id, instance)
        }

    Await.result(future, 10 minutes)
  }

  def refresh: Unit = this.synchronized {
    refresh(cache)
  }

  protected def getAllIds: Future[Traversable[ID]]

  protected def createInstance(id: ID): Future[Option[T]]

  protected def createInstances(
    ids: Traversable[ID]
  ): Future[Traversable[(ID, T)]] =
    Future.sequence(
      ids.map(id =>
        createInstance(id).map(_.map(instance => (id, instance)))
      )
    ).map(_.flatten)
}