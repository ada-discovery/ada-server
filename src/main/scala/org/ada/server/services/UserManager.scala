package org.ada.server.services

import com.google.inject.ImplementedBy
import javax.inject.{Inject, Singleton}
import org.ada.server.services.ldap.{LdapService, LdapSettings}
import org.ada.server.models.LdapUser
import org.ada.server.dataaccess.RepoTypes.UserRepo
import org.ada.server.models.User
import org.incal.core.dataaccess.Criterion.Infix
import play.api.Logger

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.concurrent.Future.sequence

@ImplementedBy(classOf[UserManagerImpl])
trait UserManager {

  /**
    * Matches a user id and password for authentication.
    */
  def authenticate(id: String, password: String): Future[Boolean]

  def synchronizeRepos: Future[Unit]

  def purgeMissing: Future[Unit]

  def lockMissing: Future[Unit]

  def findById(id: String): Future[Option[User]]

  def findByEmail(email: String): Future[Option[User]]

  def debugUsers: Traversable[User]

  val adminUser = User(None, "admin.user", "admin@mail", Seq("admin"))
  val basicUser = User(None, "basic.user", "basic@mail", Seq("basic"))
}

/**
  * Class for managing and accessing Users.
  * Interface between Ldap and local user database.
  */
@Singleton
private class UserManagerImpl @Inject()(
    userRepo: UserRepo,
    ldapService: LdapService,
    ldapSettings: LdapSettings
  ) extends UserManager {

  private val logger = Logger

  override def debugUsers: Traversable[User] =
    if (ldapSettings.addDebugUsers) {
      // add admin and basic users
      addUserIfNotPresent(adminUser)
      addUserIfNotPresent(basicUser)

      Seq(adminUser, basicUser)
    } else
      Nil

  /**
    * Synchronize user entries of LDAP server and local database.
    * Users present in LDAP server but not present in database will be added.
    * Users present in LDAP server and database are synchronized by taking credentials from LDAP and keeping roles and permissions from local database.
    * TODO change to pass arbitrary user repo
    */
  override def synchronizeRepos: Future[Unit] = {
    val futures = ldapService.listUsers.map { ldapUser: LdapUser =>
      for {
        found <- userRepo.find(Seq("ldapDn" #== ldapUser.uid)).map(_.headOption)
        _ <- found match {
          case Some(usr) =>
            userRepo.update(usr.copy(ldapDn = ldapUser.uid, email = ldapUser.email))
          case None =>
            userRepo.save(User(None, ldapUser.uid, ldapUser.email, Seq(), Seq()))
        }
      } yield
        ()
    }
    sequence(futures).map(_ => ())
  }

  /**
    * Removes local users that do not exist on the LDAP server.
    * Use this to clean the user data base or when moving from debug to production.
    * This will also remove all debug users!
    */
  override def purgeMissing =
    for {
      // local users
      localUsers <- userRepo.find()

      // retrieve all LDAP users and remove those who are not matched
      _ <- {
        val ldapUserUids = ldapService.listUsers.map(_.uid).toSet
        val nonMatchingLocalUsers = localUsers.filterNot(user => ldapUserUids.contains(user.ldapDn))
        val nonMatchingIds = nonMatchingLocalUsers.map(_._id.get)

        userRepo.delete(nonMatchingIds).map(_ =>
          logger.info(s"${nonMatchingIds.size} users missing on the LDAP server have been purged locally.")
        )
      }
    } yield
      ()

  /**
    * Locks local users that do not exist on the LDAP server.
    */
  override def lockMissing =
    for {
      // local users
      localUsers <- userRepo.find()

      // retrieve all LDAP users and remove those who are not matched
      _ <- {
        val ldapUserUids = ldapService.listUsers.map(_.uid).toSet
        val nonMatchingLocalUsers = localUsers.filterNot(user => ldapUserUids.contains(user.ldapDn))
        val usersToLock = nonMatchingLocalUsers.map(_.copy(locked = true))

        userRepo.update(usersToLock).map(_ =>
          logger.info(s"${usersToLock.size} users missing on the LDAP server have been locked.")
        )
      }
    } yield
      ()

  /**
    * Authenticate user.
    *
    * @param id ID (e.g. mail) for matching.
    * @param password Password which should match the password associated to the mail.
    * @return None, if password is wrong or not associated mail was found.
    */
  override def authenticate(id: String, password: String): Future[Boolean] =
    Future {
//      val exists = ldapUserService.getAll.find(_.uid == id).nonEmpty
      ldapService.canBind(id, password)
    }

  private def addUserIfNotPresent(user: User) =
    userRepo.find(Seq("ldapDn" #== user.ldapDn)).map { users =>
      if (users.isEmpty)
        userRepo.save(user)
    }

  /**
    * Given a mail, find the corresponding account.
    *
    * @param email mail to be matched.
    * @return Option containing Account with matching mail; None otherwise
    */
  override def findByEmail(email: String): Future[Option[User]] =
    userRepo.find(Seq("email" #== email)).map(_.headOption)

  /**
    * Given an id, find the corresponding account.
    *
    * @param id ID to be matched.
    * @return Option containing Account with matching ID; None otherwise
    */
  override def findById(id: String): Future[Option[User]] =
    userRepo.find(Seq("ldapDn" #== id)).map(_.headOption)
}