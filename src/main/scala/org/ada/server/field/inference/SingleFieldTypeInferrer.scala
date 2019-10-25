package org.ada.server.field.inference

import org.ada.server.calc.{Calculator, NoOptionsCalculatorTypePack}
import org.ada.server.field.FieldType

trait SingleFieldTypeInferrerPack[T] extends NoOptionsCalculatorTypePack{
  type IN = T
  type OUT = Option[FieldType[_]]
}

object SingleFieldTypeInferrer {
  type of[T] = Calculator[_ <: SingleFieldTypeInferrerPack[T]]
}