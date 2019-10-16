package org.ada.server.services

import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

object GuicePlayTestApp {
  lazy val modules = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val env = play.api.Environment.simple(mode = play.api.Mode.Test)
    val config = play.api.Configuration.load(env)
    config.getStringList("play.modules.enabled").fold(
      List.empty[String])(l => iterableAsScalaIterable(l).toList)
  }

  def apply(moduleNames: Seq[String] = modules): Application = {
    new GuiceApplicationBuilder()
      .configure("play.modules.enabled" -> moduleNames)
      .build
  }
}