package org.ada.server.dataaccess

import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

class SerializableApplicationLifecycle extends ApplicationLifecycle with Serializable { // addStopHookX: (() => Future[Unit]) => Unit
//  override def addStopHook(hook: () => Future[Unit]): Unit = () // addStopHookX(hook)
  override def addStopHook(hook: () => Future[_]): Unit = ()
}
