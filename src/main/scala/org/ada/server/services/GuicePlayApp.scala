package org.ada.server.services

import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

object GuicePlayTestApp {

  def apply(moduleNames: Seq[String] = Nil): Application = {
    val env = play.api.Environment.simple(mode = play.api.Mode.Test)
    val config = play.api.Configuration.load(env)

    val modules =
      if (moduleNames.nonEmpty) {
        moduleNames
      } else {
        import scala.collection.JavaConversions.iterableAsScalaIterable
        config.getStringList("play.modules.enabled").fold(
          List.empty[String])(l => iterableAsScalaIterable(l).toList)
      }
    new GuiceApplicationBuilder().configure("play.modules.enabled" -> modules).build
  }
}