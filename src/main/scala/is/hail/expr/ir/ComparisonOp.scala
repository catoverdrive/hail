package is.hail.expr.ir

import is.hail.annotations.CodeOrdering
import is.hail.asm4s.Code
import is.hail.expr.types._
import is.hail.utils.lift

object ComparisonOp {

  private def checkCompatible[T](lt: Type, rt: Type): Unit =
    if (lt != rt)
      throw new RuntimeException(s"Cannot compare types $lt and $rt")

  val fromStringAndTypes: PartialFunction[(String, Type, Type), ComparisonOp] = {
    case ("==", t1, t2) =>
      checkCompatible(t1, t2)
      EQ(t1)
    case ("!=", t1, t2) =>
      checkCompatible(t1, t2)
      NEQ(t1)
    case (">=", t1, t2) =>
      checkCompatible(t1, t2)
      GTEQ(t1)
    case ("<=", t1, t2) =>
      checkCompatible(t1, t2)
      LTEQ(t1)
    case (">", t1, t2) =>
      checkCompatible(t1, t2)
      GT(t1)
    case ("<", t1, t2) =>
      checkCompatible(t1, t2)
      LT(t1)
  }
}

sealed trait ComparisonOp {
  def typ: Type
  def op: CodeOrdering.Op
  val missingGreatest: Boolean = true
  val strict: Boolean = true
  def codeOrdering(mb: EmitMethodBuilder): CodeOrdering.F[Boolean] = {
    mb.getCodeOrdering[Boolean](typ, op, missingGreatest = missingGreatest)
  }
}

case class GT(typ: Type) extends ComparisonOp {
  val op: CodeOrdering.Op = CodeOrdering.gt
  override val missingGreatest: Boolean = false
}
case class GTEQ(typ: Type) extends ComparisonOp {
  val op: CodeOrdering.Op = CodeOrdering.gteq
  override val missingGreatest: Boolean = false
}
case class LTEQ(typ: Type) extends ComparisonOp { val op: CodeOrdering.Op = CodeOrdering.lteq }
case class LT(typ: Type) extends ComparisonOp { val op: CodeOrdering.Op = CodeOrdering.lt }
case class EQ(typ: Type) extends ComparisonOp {
  val op: CodeOrdering.Op = CodeOrdering.equiv
  override val strict: Boolean = false
}
case class NEQ(typ: Type) extends ComparisonOp {
  val op: CodeOrdering.Op = CodeOrdering.neq
  override val strict: Boolean = false
}