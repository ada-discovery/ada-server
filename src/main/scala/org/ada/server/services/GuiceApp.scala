package org.ada.server.services

import com.google.inject.{Guice, Injector, Module}

object GuiceApp {
  def apply(modules: Seq[Module]): Injector = {
    Guice.createInjector(modules:_*)
  }
}