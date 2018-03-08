package is.hail.expr.ir.functions

import is.hail.asm4s._
import is.hail.expr.types._
import is.hail.expr.ir._

object GenotypeFunctions {

  def gqFromPL(pl: IR): IR = {
    val body =
      If(ApplyBinaryPrimOp(LT(), Ref("v"), GetTupleElement(Ref("m"), 0)),
        MakeTuple(Seq(Ref("v"), GetTupleElement(Ref("m"), 0))),
        If(ApplyBinaryPrimOp(LT(), Ref("v"), GetTupleElement(Ref("m"), 1)),
          MakeTuple(Seq(GetTupleElement(Ref("m"), 0), Ref("v"))),
          Ref("m")))
    Let("mtup",
      ArrayFold(pl, MakeTuple(Seq(I32(99), I32(99))), "m", "v", body),
      ApplyBinaryPrimOp(Subtract(), GetTupleElement(Ref("mtup"), 1), GetTupleElement(Ref("mtup"), 0)))
  }
}

class GenotypeFunctions {

}
