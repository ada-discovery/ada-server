package org.ada.server.dataaccess.ignite

import org.ada.server.dataaccess.mongo.ReactiveMongoApi
import org.apache.ignite.lifecycle.{LifecycleEventType, LifecycleBean}

class IgniteLifecycleBean extends LifecycleBean {

  override def onLifecycleEvent(evt: LifecycleEventType) = {
    println("onLifecycleEvent called " + evt)
    if (evt == LifecycleEventType.AFTER_NODE_STOP) {
      ReactiveMongoApi.get.foreach { mongoApi =>
        println("closing Mongo Ignite connections")
        mongoApi.connection.close
        mongoApi.driver.close()
      }
    }
  }
}