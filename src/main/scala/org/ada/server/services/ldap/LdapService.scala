package org.ada.server.services.ldap

import javax.inject.{Inject, Singleton}
import com.google.inject.ImplementedBy
import com.unboundid.ldap.sdk._
import org.ada.server.AdaException
import org.ada.server.models.{LdapUser, UserGroup}
import play.api.Logger
import play.api.inject.ApplicationLifecycle
import org.ada.server.services.ldap.LDAPInterfaceFactory._

import scala.collection.JavaConversions._

@ImplementedBy(classOf[LdapServiceImpl])
trait LdapService {

  def listUsers: Traversable[LdapUser]

  def canBind(userDN: String, password: String): Boolean

  // unused
  def listEntries: Traversable[String]
  def getEntry(dn: String): Option[Entry]
}

@Singleton
protected class LdapServiceImpl @Inject()(
  settings: LdapSettings,
  applicationLifecycle: ApplicationLifecycle
) extends LdapService {

  // interface for use
  private val interface: Option[LDAPInterface] = setupInterface
  private val connectionOptions = createConnectionOptions
  private val logger = Logger

  override def listUsers: Traversable[LdapUser] = {
    val personFilter = Filter.createEqualityFilter("objectClass", "person")

    val filterUsers = if(settings.groups.nonEmpty) {
      val memberFilter: Seq[Filter] = settings.groups.map{ groupname =>
        Filter.createEqualityFilter("memberof", groupname)
      }
      Filter.createANDFilter(Filter.createORFilter(memberFilter), personFilter)
    } else
      personFilter

    val request: SearchRequest = new SearchRequest(settings.dit, SearchScope.SUB, filterUsers)
    val entries: Traversable[Entry] = search(request)
    entries.map(entryToLdapUser).flatten
  }

  /**
    * Reconstruct CustomUser from ldap entry.
    * Use this convert SearchResultEntry or others to CustomUser.
    * If the entry does not point to a user, a CustomUser with null fields will be created.
    *
    * @param entry Entry as input for reconstruction.
    * @return CustomUser, if Entry is not null, None else.
    */
  private def entryToLdapUser(entry: Entry): Option[LdapUser] =
    Option(entry).map { entry =>
      val uid: String = entry.getAttributeValue("uid")
      val name: String = entry.getAttributeValue("cn")
      val email: String = entry.getAttributeValue("mail")
      val ou: String = entry.getAttributeValue("ou")
      val permissions: Array[String] = Option(entry.getAttributeValues("memberof")).getOrElse(Array())
      LdapUser(uid, name, email, ou, permissions)
    }

  /**
    * Convert entry to UserGroup object if possible.
    *
    * @param entry Ldap entry to be converted.
    * @return UserGroup, if conversion possible, None else.
    */
  private def entryToUserGroup(entry: Entry): Option[UserGroup] =
    Option(entry) match {
      case None => None
      case _ =>
        val name: String = entry.getAttributeValue("cn")
        val members: Array[String] = Option(entry.getAttributeValues("member")).getOrElse(Array())
        val description: Option[String] = Option(entry.getAttributeValue("description"))
        val nested: Array[String] = Option(entry.getAttributeValues("memberof")).getOrElse(Array())
        Some(UserGroup(None, name, description, members, nested))
    }

  /**
    * Creates either a server or a connection, depending on the configuration.
    * @return LDAPInterface, either of type InMemoryDirectoryServer or LDAPConnection.
    */
  private def setupInterface: Option[LDAPInterface] =
    settings.mode match {

      case "local" =>
        Some(
          LDAPInterfaceFactory.createLocalServer(settings.dit, settings.port, applicationLifecycle)
        )

      case "remote" =>
        val password = settings.bindPassword.getOrElse(
          throw new AdaException("Environmental variable 'ADA_LDAP_BIND_PASSWORD' or a conf entry 'ldap.bindPassword' not set but expected.")
        )

        LDAPInterfaceFactory.createConnectionPool(
          settings.host, settings.port, settings.encryption, settings.trustStore, settings.bindDN, password, applicationLifecycle,
          connectionOptions
        )

      case _ => None
    }

  /**
    * Establish connection and check if bind possible.
    * Useful for authentication.
    * @param id user id for binding.
    * @param password password for binding.
    * @return true, if bind successful.
    */
  override def canBind(uid: String, password: String): Boolean =
    if (settings.recursiveDitAuthenticationSearch) {
      listUserSubEntries(uid, settings.dit).headOption.map { userEntry =>
        canBindAux(userEntry.getDN, password)
      }.getOrElse(false)
    } else {
      val userDN = "uid=" + uid + "," + settings.dit
      canBindAux(userDN, password)
    }

  private def canBindAux(userDN: String, password: String): Boolean = {
    logger.info(s"Checking the user credentials for $userDN in LDAP.")

    val connection = settings.mode match {

      case "local" =>
        Some(
          createConnection("localhost", settings.port, settings.encryption, settings.trustStore, connectionOptions)._1
        )

      case "remote" =>
        Some(
          createConnection(settings.host, settings.port, settings.encryption, settings.trustStore, connectionOptions)._1
        )

      case _ =>
        None
    }

    val result: ResultCode = connection.map { connection =>
      try {
        connection.bind(userDN, password).getResultCode
      } catch {
        case _: LDAPException => ResultCode.NO_SUCH_OBJECT
      } finally {
        // Close the connection
        connection.close()
      }
    }.getOrElse(
      ResultCode.AUTH_UNKNOWN
    )

    // Log the outcome
    val successfulWord = if (result == ResultCode.SUCCESS) "successful" else "unsuccessful"
    logger.info(s"The LDAP verification for $userDN is ${successfulWord.toUpperCase}.")

    result == ResultCode.SUCCESS
  }

  private def listUserSubEntries(uid: String, baseDN: String): Traversable[Entry] = {
    val personFilter = Filter.createEqualityFilter("objectClass", "person")
    val idFilter = Filter.createEqualityFilter("uid", uid)
    val idPersonFilter = Filter.createANDFilter(idFilter, personFilter)

    val request = new SearchRequest(baseDN, SearchScope.SUB, idPersonFilter)
    search(request)
  }

  /**
    * Find Entry based on its DN.
    *
    * @param dn Distinguished name for search operation.
    * @return Entry wrapped in Option if found; None else.
    */
  override def getEntry(dn: String): Option[Entry] =
    interface.flatMap( interface =>
      Option(interface.getEntry(dn))
    )

  /**
    * For debugging purposes.
    * Gets list of all entries.
    *
    * @return List of ldap entries.
    */
  override def listEntries: Traversable[String] =
    interface match {
      case Some(interface) =>
        val searchRequest: SearchRequest = new SearchRequest(settings.dit, SearchScope.SUB, Filter.create("(objectClass=*)"))
        val entries: Traversable[Entry] = dispatchSearchRequest(interface, searchRequest)
        entries.map(_.toString)

      case None => Nil
    }

  /**
    * Dispatch SearchRequest and return list of found entries.
    * @param searchRequest Request to be dipatched.
    * @return List of matching entries.
    */
  private def search(searchRequest: SearchRequest): Traversable[Entry] =
    interface match {
      case Some(interface) => dispatchSearchRequest(interface, searchRequest)
      case None => Nil
    }

  /**
    * Secure, crash-safe ldap search method.
    *
    * @param interface interface to perform search request on.
    * @param request SearchRequest to be executed.
    * @return List of search results. Empty, if request failed.
    */
  private def dispatchSearchRequest(
    interface: LDAPInterface,
    request: SearchRequest
  ): Traversable[Entry] =
    try {
      interface.search(request).getSearchEntries
    } catch {
      case e: Throwable => Nil
    }

  private def createConnectionOptions: LDAPConnectionOptions = {
    val options = new LDAPConnectionOptions()

    settings.connectTimeout.foreach(
      options.setConnectTimeoutMillis(_)
    )

    settings.responseTimeout.foreach(
      options.setResponseTimeoutMillis(_)
    )

    settings.pooledSchemaTimeout.foreach(
      options.setPooledSchemaTimeoutMillis(_)
    )

    settings.abandonOnTimeout.foreach(
      options.setAbandonOnTimeout(_)
    )

    options
  }
}