package org.ada.server.models

import java.util.Date

import org.ada.server.dataaccess.BSONObjectIdentity
import org.ada.server.json.EnumFormat
import org.incal.core.{ConditionType, FilterCondition}
import play.api.libs.json.{Format, Json, __}
import play.api.libs.functional.syntax._
import reactivemongo.bson.BSONObjectID
import reactivemongo.play.json.BSONFormats.BSONObjectIDFormat

case class Filter(
  _id: Option[BSONObjectID] = None,
  name: Option[String] = None,
  conditions: Seq[FilterCondition] = Nil,
  isPrivate: Boolean = false,
  createdById: Option[BSONObjectID] = None,
  timeCreated: Option[Date] = Some(new Date()),
  var createdBy: Option[User] = None
) {
  def conditionsOrId: Either[Seq[FilterCondition], BSONObjectID] =
    _id.fold(Left(conditions): Either[Seq[FilterCondition], BSONObjectID]){filterId => Right(filterId)}
}

object FilterShowFieldStyle extends Enumeration {
  val NamesOnly, LabelsOnly, LabelsAndNamesOnlyIfLabelUndefined, NamesAndLabels = Value
}

object Filter {

  implicit val conditionTypeFormat = EnumFormat(ConditionType)

  implicit val filterConditionFormat = Json.format[FilterCondition]

  implicit val filterFormat: Format[Filter] = (
    (__ \ "_id").formatNullable[BSONObjectID] and
      (__ \ "name").formatNullable[String] and
      (__ \ "conditions").format[Seq[FilterCondition]] and
      (__ \ "isPrivate").format[Boolean] and
      (__ \ "createdById").formatNullable[BSONObjectID] and
      (__ \ "timeCreated").formatNullable[Date]
    )(
    Filter(_, _, _, _, _, _),
    (item: Filter) =>  (item._id, item.name, item.conditions, item.isPrivate, item.createdById, item.timeCreated)
  )

  implicit object FilterIdentity extends BSONObjectIdentity[Filter] {
    def of(entity: Filter): Option[BSONObjectID] = entity._id
    protected def set(entity: Filter, id: Option[BSONObjectID]) = entity.copy(_id = id)
  }

  def apply(conditions: Seq[FilterCondition]): Option[Filter] =
    conditions match {
      case Nil => None
      case _ => Some(Filter(conditions = conditions))
    }

  type FilterOrId = Either[Seq[FilterCondition], BSONObjectID]
}