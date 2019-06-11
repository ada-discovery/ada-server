package org.ada.server.services

import ConfigModule.{ConfigProvider, ConfigurationProvider}
import com.google.inject.{AbstractModule, Provider}
import com.typesafe.config.{Config, ConfigFactory}
import javax.inject.{Inject, Singleton}
import net.codingwell.scalaguice.ScalaModule
import play.api.inject.{ApplicationLifecycle, DefaultApplicationLifecycle, Module}
import play.api.{Configuration, Environment}

// TODO: Do we need it?
object ConfigModule {
  private class ConfigProvider extends Provider[Config] {
    override def get = ConfigFactory.load()
  }

  private class ConfigurationProvider @Inject()(config: Config) extends Provider[Configuration] {
    override def get = new Configuration(config)
  }
}

class ConfigModule extends ScalaModule {

  override def configure() {
    bind[Config].toProvider[ConfigProvider].asEagerSingleton()
    bind[Configuration].toProvider[ConfigurationProvider].asEagerSingleton()
    bind[Environment].toInstance(Environment.simple())
    bind[ApplicationLifecycle].toInstance(new DefaultApplicationLifecycle())
  }
}