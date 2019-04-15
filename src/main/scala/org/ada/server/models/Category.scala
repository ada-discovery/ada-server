package org.ada.server.models

import reactivemongo.bson.BSONObjectID

case class Category(
  _id: Option[BSONObjectID],
  name: String,
  label: Option[String] = None,
  var parentId: Option[BSONObjectID] = None,
  var parent: Option[Category] = None,
  var children: Seq[Category] = Seq[Category]()
) {
  def this(name : String) = this(None, name)

  def getPath: Seq[String] =
    (
      if (parent.isDefined && parent.get.parent.isDefined)
        parent.get.getPath
      else
        Seq[String]()
      ) ++ Seq(name)

  def getLabelPath: Seq[String] =
    (
      if (parent.isDefined && parent.get.parent.isDefined)
        parent.get.getLabelPath
      else
        Seq[String]()
      ) ++ Seq(labelOrElseName)

  def setChildren(newChildren: Seq[Category]): Category = {
    children = newChildren
    children.foreach(_.parent = Some(this))
    this
  }

  def addChild(newChild: Category): Category = {
    children = Seq(newChild) ++ children
    newChild.parent = Some(this)
    this
  }

  def labelOrElseName = label.getOrElse(name)

  override def toString = name

  override def hashCode = name.hashCode
}
