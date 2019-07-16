package org.ada.server.dataaccess.ignite

import org.ada.server.dataaccess.mongo.ReactiveMongoApi
import org.apache.ignite.lifecycle.{LifecycleBean, LifecycleEventType}
import scala.concurrent.duration._
import play.api.libs.concurrent.Execution.Implicits.defaultContext

class IgniteLifecycleBean extends LifecycleBean {

  override def onLifecycleEvent(evt: LifecycleEventType) = {
    println("onLifecycleEvent called " + evt)
    if (evt == LifecycleEventType.AFTER_NODE_STOP) {
      ReactiveMongoApi.get.foreach { mongoApi =>
        println("closing Mongo Ignite connections")
        mongoApi.connection.askClose()(5 minutes).map( _=>
          mongoApi.driver.close()
        )
      }
    }
  }
}