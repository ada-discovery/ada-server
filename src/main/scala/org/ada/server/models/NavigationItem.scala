package org.ada.server.models

import play.api.libs.json.{Format, JsObject, Json}
import org.ada.server.json.{RuntimeClassFormat, SubTypeFormat}

trait NavigationItem

case class Link(label: String, url: String) extends NavigationItem
case class Menu(header: String, links: Seq[Link]) extends NavigationItem

object Link {
  // for some reason the link format must be defined separately and then imported bellow otherwise Seq[Link] format is not picked up
  implicit val linkFormat = Json.format[Link]
}

object NavigationItem {
  import Link.linkFormat
  implicit val menuFormat = Json.format[Menu]

  implicit val navigationItemFormat: Format[NavigationItem] = new SubTypeFormat[NavigationItem](
    Seq(
      RuntimeClassFormat(linkFormat),
      RuntimeClassFormat(menuFormat)
    )
  )
}