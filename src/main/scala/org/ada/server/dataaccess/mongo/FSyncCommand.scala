package org.ada.server.dataaccess.mongo

import reactivemongo.api.commands.{CollectionCommand, Command, CommandWithResult, UnitBox}
import reactivemongo.bson.BSONDocument

case class FSyncCommand(
    async: Boolean = true,
    lock: Boolean = false
  ) extends Command with CommandWithResult[UnitBox.type] {

  def toBSON: BSONDocument =
    BSONDocument("fsync" -> 1, "async" -> async, "lock" -> lock)
}
