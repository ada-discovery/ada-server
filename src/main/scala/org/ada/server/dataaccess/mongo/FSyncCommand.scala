package org.ada.server.dataaccess.mongo

import reactivemongo.api.BSONSerializationPack
import reactivemongo.api.commands.{CollectionCommand, Command, CommandWithResult, UnitBox}
import reactivemongo.bson.BSONDocument

case class FSyncCommand(
  async: Boolean = true,
  lock: Boolean = false
) extends Command with CommandWithResult[UnitBox.type]

object FSyncCommand {

  implicit object FSyncCommandWriter extends BSONSerializationPack.Writer[FSyncCommand] {
    override def write(t: FSyncCommand): BSONDocument = BSONDocument("fsync" -> 1, "async" -> t.async, "lock" -> t.lock)
  }

  implicit object UnitBoxReader extends BSONSerializationPack.Reader[UnitBox.type] {
    override def read(bson: BSONDocument) = UnitBox
  }
}
