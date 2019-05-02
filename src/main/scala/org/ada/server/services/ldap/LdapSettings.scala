package org.ada.server.services.ldap

import com.google.inject.{Inject, Singleton}
import com.typesafe.config.ConfigValue
import play.api.Configuration

@Singleton
class LdapSettings @Inject()(configuration: Configuration) extends Enumeration{
  // general settings
  // dit denotes the branch of the directory tree which is to be used
  // groups defines which user groups are to be used for authentication
  val dit: String = configuration.getString("ldap.dit").getOrElse("cn=users,cn=accounts,dc=ada")
  val groups: Seq[String] = configuration.getStringSeq("ldap.groups").getOrElse(Seq())
  val recursiveDitAuthenticationSearch = configuration.getBoolean("ldap.recursiveDitAuthenticationSearch").getOrElse(false)
  val addDebugUsers: Boolean = configuration.getBoolean("ldap.debugusers").getOrElse(false)
  // switch for local ldap server or connection to remote server
  // use "local" to set up local in-memory server
  // use "remote" to set up connection to remote server
  // use "none" to disable this module completely
  // defaults to "local", if no option is given
  val mode: String = configuration.getString("ldap.mode").getOrElse("local").toLowerCase()
  val host: String = configuration.getString("ldap.host").getOrElse("localhost")
  val port: Int = configuration.getInt("ldap.port").getOrElse(389)
  val timeout: Int = configuration.getInt("ldap.timeout").getOrElse(2000)
  val bindDN: String = configuration.getString("ldap.bindDN").getOrElse("cn=admin.user,dc=users," + dit)
  val bindPassword: Option[String] = configuration.getString("ldap.bindPassword")

  // encryption settings
  // be aware that by default, client certificates are disabled and server certificates are always trusted!
  // do not use remote mode unless you know the server you connect to!
  val encryption: String = configuration.getString("ldap.encryption").getOrElse("none").toLowerCase()
  val trustStore: Option[String] = configuration.getString("ldap.trustStore")

  // time-out settings
  val connectTimeout: Option[Int] = configuration.getInt("ldap.connectTimeout")
  val responseTimeout: Option[Long] = configuration.getLong("ldap.responseTimeout")
  val pooledSchemaTimeout : Option[Long] = configuration.getLong("ldap.pooledSchemaTimeout")
  val abandonOnTimeout: Option[Boolean] = configuration.getBoolean("ldap.abandonOnTimeout")

  def toList(): List[(String, ConfigValue)] = {
    val subconfig: Option[Configuration] = configuration.getConfig("services/ldap")
    subconfig match{
      case Some(c) => c.entrySet.toList
      case None => List()
    }
  }
}
