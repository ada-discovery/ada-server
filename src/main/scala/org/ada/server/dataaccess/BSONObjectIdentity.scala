package org.ada.server.dataaccess

import org.incal.core.Identity
import reactivemongo.bson.BSONObjectID

trait BSONObjectIdentity[E] extends Identity[E, BSONObjectID] {
  val name = "_id" // must be like that!
  def next = BSONObjectID.generate
}