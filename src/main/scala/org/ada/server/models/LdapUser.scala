package org.ada.server.models

import play.api.libs.json.{Json, Format}

/**
  * Holds information that could be extracted from the LDAP server.
  *
  * @param uid LDAP DN
  * @param name  common name (cn)
  * @param email email address
  * @param ou organisatorical unit (ou)
  * @param permissions LDAP groups (memberof)
  */
case class LdapUser(uid: String, name: String, email: String, ou: String, permissions: Seq[String])

object LdapUser {
  implicit val ldapUserFormat: Format[LdapUser] = Json.format[LdapUser]
}
