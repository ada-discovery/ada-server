package org.ada.server

import com.google.inject.Module
import org.ada.server.services.GuicePlayTestApp
import play.api.inject.guice.GuiceApplicationBuilder

import scala.reflect.ClassTag

/**
  * Helper class for using DI in tests.
  */
class Injector(modules: Module*) {
  // TODO: Replace this with the Guice injector when Play DI is removed from ada-server
  private val injector = new GuiceApplicationBuilder()
    .configure("play.modules.enabled" -> GuicePlayTestApp.modules)
    .bindings(modules)
    .build
    .injector

  def getInstance[T: ClassTag]: T = {
    injector.asInstanceOf[T]
  }
}
