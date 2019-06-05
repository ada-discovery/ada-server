package org.ada.server.services.ldap

import javax.net.ssl.{SSLContext, SSLSocketFactory}
import com.unboundid.ldap.listener.{InMemoryDirectoryServer, InMemoryDirectoryServerConfig, InMemoryListenerConfig}
import com.unboundid.ldap.sdk.extensions.StartTLSExtendedRequest
import com.unboundid.ldap.sdk.{LDAPConnection, LDAPConnectionOptions, _}
import com.unboundid.util.ssl.{SSLUtil, TrustAllTrustManager, TrustStoreTrustManager}
import play.api.Logger
import play.api.inject.ApplicationLifecycle

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object LDAPInterfaceFactory {

  /**
    * Creates an LDAP in-memory server for testing.
    * Builds user permissions and roles from PermissionCache and RoleCache.
    * Feed users from user database into server.
    * @return dummy server
    */
  def createLocalServer(
    dit: String,
    port: Int,
    applicationLifecycle: ApplicationLifecycle
  ): LDAPInterface = {
    // setup configuration
    val config = new InMemoryDirectoryServerConfig(dit)
    config.setSchema(null); // do not check (attribute) schema
    config.setAuthenticationRequiredOperationTypes(OperationType.DELETE, OperationType.ADD, OperationType.MODIFY, OperationType.MODIFY_DN)

    // required for interaction; commented out for debugging reasons
    val listenerConfig = new InMemoryListenerConfig("defaultListener", null, port, null, null, null);
    config.setListenerConfigs(listenerConfig)

    val server = new InMemoryDirectoryServer(config)
    server.startListening()

    // initialize ldap structures

    // add root
    server.add("dn: " + dit, "objectClass:top", "objectClass:domain", dit.replace("=",":"))
    // add subtrees: roles, permissions, people
    server.add("dn:dc=groups," + dit, "objectClass:top", "objectClass:domain", "dc:roles")
    server.add("dn:dc=users," + dit, "objectClass:top", "objectClass:domain", "dc:users")

    // hook interface in lifecycle for proper cleanup
    applicationLifecycle.addStopHook( () => Future(terminateInterface(server)))

    Logger.info(s"Local LDAP server started at the port $port.")

    server
  }

  /**
    * Creates a connection pool to an existing LDAP server instance.
    * We use ConnectionPools for better performance.
    * Uses the options defined in the configuation.
    * Used options from configuration are ldap.encryption, ldap.host, ldap.prt, ldap.bindDN, ldap.bindPassword
    * @param bindDN custom bindDn
    * @param password custom bind password
    * @return LDAPConnectionPool object with specified credentials. None, if no connection could be established.
    */
  def createConnectionPool(
    host: String,
    port: Int,
    encryption: String,
    trustStore: Option[String],
    bindDN: String,
    password: String,
    applicationLifecycle: ApplicationLifecycle,
    options: LDAPConnectionOptions = new LDAPConnectionOptions()
  ): Option[LDAPConnectionPool] = {
    val (connection, processor) = createConnection(host, port, encryption, trustStore, options)

    val result: ResultCode = try {
      connection.bind(bindDN, password).getResultCode
    } catch {
      case _: Throwable => ResultCode.NO_SUCH_OBJECT
    }

    if (result == ResultCode.SUCCESS) {
      Logger.info(s"${encryption} LDAP connection to " + host + ":" + port + " established")

      Some(
        createConnectionPool(connection, processor, applicationLifecycle)
      )
    } else {
      Logger.warn("Failed to establish connection to " + host + ":" + port)
      None
    }
  }

  /**
    * Creates a connection to an existing LDAP server instance.
    * @return LDAPConnection object
    */
  def createConnection(
    host: String,
    port: Int,
    encryption: String,
    trustStore: Option[String],
    options: LDAPConnectionOptions = new LDAPConnectionOptions()
  ): (LDAPConnection, Option[PostConnectProcessor]) = {
    val sslUtil: SSLUtil = setupSSLUtil(trustStore)

    encryption match {
      case "ssl" =>
        // connect to server with ssl encryption
        val sslSocketFactory: SSLSocketFactory = sslUtil.createSSLSocketFactory()
        val connection = new LDAPConnection(sslSocketFactory, options, host, port)
        (connection, None)

      case "starttls" =>
        // connect to server with starttls connection
        val connection: LDAPConnection = new LDAPConnection(options, host, port)

        val sslContext: SSLContext = sslUtil.createSSLContext()
        connection.processExtendedOperation(new StartTLSExtendedRequest(sslContext))
        val processor = new StartTLSPostConnectProcessor(sslContext)
        (connection, Some(processor))

      case _ =>
        // create unsecured connection
        val connection = new LDAPConnection(options, host, port)
        (connection, None)
    }
  }

  private def createConnectionPool(
    connection: LDAPConnection,
    processor: Option[PostConnectProcessor],
    applicationLifecycle: ApplicationLifecycle
  ) = {
    val connectionPool = processor.map(
      new LDAPConnectionPool(connection, 1, 10, _)
    ).getOrElse(
      new LDAPConnectionPool(connection, 1, 10)
    )

    // hook interface in lifecycle for proper cleanup
    applicationLifecycle.addStopHook( () => Future(terminateInterface(connectionPool)))

    connectionPool
  }

  /**
    * Setup SSL context (e.g for use with startTLS).
    * If a truststore file has been defined in the config, it will be loaded.
    * Otherwise, server certificates will be blindly trusted.
    * @return Created SSLContext.
    */
  private def setupSSLUtil(trustStore: Option[String]): SSLUtil =
    trustStore match{
      case Some(path) => new SSLUtil(new TrustStoreTrustManager(path))
      case None => new SSLUtil(new TrustAllTrustManager())
    }


  /**
    * Closes LDAPConnection or shuts down InMemoryDirectoryServer.
    * Ensures that application releases ports.
    * @param interface Interface to be disconnected or shut down.
    */
  private def terminateInterface(interface: LDAPInterface) =
    interface match {
      case server: InMemoryDirectoryServer => server.shutDown(true)
      case connection: LDAPConnection => connection.close()
      case connectionPool: LDAPConnectionPool => connectionPool.close()
      case _ => Unit
    }
}