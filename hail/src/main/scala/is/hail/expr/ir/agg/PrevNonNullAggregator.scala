package is.hail.expr.ir.agg

import is.hail.annotations.{Region, RegionUtils, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitFunctionBuilder, EmitTriplet, typeToTypeInfo}
import is.hail.expr.types.physical._
import is.hail.utils._

class PrevNonNullAggregator(typ: PType) extends StagedAggregator {
  type State = TypedRVAState2
  val resultType: PType = typ

  def createState(fb: EmitFunctionBuilder[_]): State =
    new TypedRVAState2(typ, fb)

  def initOp(state: State, init: Array[EmitTriplet], dummy: Boolean): Code[Unit] = {
    assert(init.length == 0)
    state.setMissing()
  }

  def seqOp(state: State, seq: Array[EmitTriplet], dummy: Boolean): Code[Unit] = {
    val Array(elt: EmitTriplet) = seq

    val v = state.fb.newField(typeToTypeInfo(typ))

    Code(
      elt.setup,
      elt.m.mux(
        Code._empty,
        Code(
          v := elt.value,
          state.setValue(v))))
  }

  def combOp(state: State, other: State, dummy: Boolean): Code[Unit] = {
    other.isMissing().mux(
      Code._empty,
      state.copyFrom(other.off))
  }

  def result(state: State, srvb: StagedRegionValueBuilder, dummy: Boolean): Code[Unit] = {
    state.isMissing().mux(
      srvb.setMissing(),
      srvb.addWithDeepCopy(resultType, state.value()))
  }
}
