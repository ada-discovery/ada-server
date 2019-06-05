package org.ada.server.services.ldap

import com.google.inject.{Inject, Singleton}
import play.api.Configuration

import scala.collection.mutable.ArrayBuffer

@Singleton
class LdapSettings @Inject()(configuration: Configuration) {

  private var settings = ArrayBuffer[(String, Any)]()

  // switch for local ldap server or connection to remote server
  // use "local" to set up local in-memory server
  // use "remote" to set up connection to remote server
  // use "none" to disable this module completely
  // defaults to "local", if no option is given
  val mode: String = conf(_.getString(_), "mode", "local")

  val host: String = conf(_.getString(_),"host", "localhost")
  val port: Int = conf(_.getInt(_), "port", 389)

  // general settings
  // dit denotes the branch of the directory tree which is to be used
  // groups defines which user groups are to be used for authentication
  val dit = conf(_.getString(_), "dit", "cn=users,cn=accounts,dc=ada")

  val groups: Seq[String] = conf(_.getStringSeq(_),"groups", Nil)

  val bindDN: String = conf(_.getString(_),"bindDN", "cn=admin.user,dc=users," + dit)
  val bindPassword: Option[String] = conf(_.getString(_), "bindPassword")

  // encryption settings
  // be aware that by default, client certificates are disabled and server certificates are always trusted!
  // do not use remote mode unless you know the server you connect to!
  val encryption: String = conf(_.getString(_), "encryption", "none")
  val trustStore: Option[String] = conf(_.getString(_),"trustStore")

  val addDebugUsers: Boolean = conf(_.getBoolean(_),"debugusers", false)

  // time-out settings
  val connectTimeout: Option[Int] = conf(_.getInt(_),"connectTimeout")
  val responseTimeout: Option[Long] = conf(_.getLong(_), "responseTimeout")
  val pooledSchemaTimeout : Option[Long] = conf(_.getLong(_), "pooledSchemaTimeout")
  val abandonOnTimeout: Option[Boolean] = conf(_.getBoolean(_), "abandonOnTimeout")

  val recursiveDitAuthenticationSearch = conf(_.getBoolean(_),"recursiveDitAuthenticationSearch", false)

  def listAll: Seq[(String, Any)] = settings.map { case (path, value) => ("ldap." + path, value) }

  private def conf[T](fun: (Configuration, String) => Option[T], path: String, default: T): T = {
    val value = fun(configuration, "ldap." + path).getOrElse(default)
    settings += ((path, value))
    value
  }

  private def conf[T](fun: (Configuration, String) => Option[T], path: String): Option[T] = {
    val value = fun(configuration, "ldap." + path)
    settings += ((path, value))
    value
  }
}
