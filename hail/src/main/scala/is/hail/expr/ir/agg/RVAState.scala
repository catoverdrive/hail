package is.hail.expr.ir.agg

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitMethodBuilder, EmitRegion}
import is.hail.expr.types.physical._
import is.hail.io.{CodecSpec, InputBuffer, OutputBuffer}
import is.hail.utils._

abstract class RVAState {
  def mb: EmitMethodBuilder
  protected def r: ClassFieldRef[Region]
  def off: ClassFieldRef[Long]
  def typ: PType
  def region: Code[Region] = r.load()

  def loadStateFrom(src: Code[Long]): Code[Unit]

  def copyFrom(src: Code[Long]): Code[Unit]

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit]

  def unserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit]

  def assign(other: Code[Region]): Code[Unit] = Code(region.isNull.mux(Code._empty, region.close()), r := other)

  def close: Code[Unit] = region.isNull.mux(Code._empty, Code(region.close(), r := Code._null))

  def er: EmitRegion = EmitRegion(mb, region)
}

case class TypedRVAState(typ: PType, mb: EmitMethodBuilder, r: ClassFieldRef[Region], off: ClassFieldRef[Long]) extends RVAState {
  def loadStateFrom(src: Code[Long]): Code[Unit] = off := src

  def copyFrom(src: Code[Long]): Code[Unit] = off := StagedRegionValueBuilder.deepCopy(er, typ, src)

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit] = {
    val enc = codec.buildEmitEncoderF[Long](typ, typ, mb.fb)
    ob: Code[OutputBuffer] => enc(region, off, ob)
  }

  def unserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit] = {
    val dec = codec.buildEmitDecoderF[Long](typ, typ, mb.fb)
    ib: Code[InputBuffer] => off := dec(region, ib)
  }
}

object StateContainer {
  def typ(n: Int): PTuple = PTuple(Array.fill(n)(PInt64()), required = true)
}

case class StateContainer(states: Array[RVAState], topRegion: Code[Region]) {
  val nStates: Int = states.length
  val typ: PTuple = StateContainer.typ(nStates)

  def apply(i: Int): RVAState = states(i)
  def getRegion(rOffset: Code[Int], i: Int): Code[Region] = topRegion.getParentReference(rOffset + i)
  def getStateOffset(off: Code[Long], i: Int): Code[Long] = typ.loadField(topRegion, off, i)
  def loadStateAddress(off: Code[Long], i: Int): Code[Long] = topRegion.loadAddress(getStateOffset(off, i))

  def setAllMissing(off: Code[Long]): Code[Unit] = toCode((i, _) => typ.setFieldMissing(topRegion, off, i))
  def setPresent(off: Code[Long], i: Int): Code[Unit] = typ.setFieldPresent(topRegion, off, i)

  def toCode(f: (Int, RVAState) => Code[Unit]): Code[Unit] =
    coerce[Unit](Code(Array.tabulate(nStates)(i => f(i, states(i))): _*))

  def loadOneIfMissing(stateOffset: Code[Long], idx: Int): Code[Unit] = {
    states(idx).region.isNull.mux(
      Code(
        typ.isFieldMissing(topRegion, stateOffset, idx).mux(Code._empty,
          states(idx).loadStateFrom(loadStateAddress(stateOffset, idx))),
        states(idx).assign(getRegion(0, idx))),
      Code._empty)
  }

  def loadRegions(rOffset: Code[Int]): Code[Unit] =
    toCode((i, s) => s.assign(getRegion(rOffset, i)))

  def load(rOffset: Code[Int], stateOffset: Code[Long]): Code[Unit] =
    Code(loadRegions(rOffset),
      toCode((i, s) => typ.isFieldMissing(topRegion, stateOffset, i).mux(
        Code._empty,
        s.loadStateFrom(loadStateAddress(stateOffset, i)))))

  def store(rOffset: Code[Int], statesOffset: Code[Long]): Code[Unit] =
    toCode((i, s) => Code(s.region.isNull.mux(Code._empty,
      Code(topRegion.setParentReference(s.region, rOffset + i),
        s.close)),
      topRegion.storeAddress(getStateOffset(statesOffset, i), s.off)))

  def addState(srvb: StagedRegionValueBuilder): Code[Unit] = {
    srvb.addBaseStruct(typ, ssb =>
      Code(ssb.start(),
        toCode((_, s) => Code(ssb.addAddress(s.off), ssb.advance()))))
  }
}