package org.ada.server.dataaccess.ignite

import org.apache.ignite.binary.{BinaryWriter, BinaryReader, BinarySerializer}
import reactivemongo.bson.BSONObjectID

class BSONObjectIDBinarySerializer extends BinarySerializer {

  private val rawField = classOf[BSONObjectID].getDeclaredField("reactivemongo$bson$BSONObjectID$$raw")
  rawField.setAccessible(true)

  override def writeBinary(obj: scala.Any, writer: BinaryWriter): Unit = {
    val objectID = obj.asInstanceOf[BSONObjectID]
    val raw = rawField.get(objectID).asInstanceOf[Array[Byte]]
    writer.writeByteArray("raw", raw)
  }

  override def readBinary(obj: scala.Any, reader: BinaryReader): Unit = {
    val objectID = obj.asInstanceOf[BSONObjectID]
    val raw = reader.readByteArray("raw")
    rawField.set(objectID, raw)
  }
}