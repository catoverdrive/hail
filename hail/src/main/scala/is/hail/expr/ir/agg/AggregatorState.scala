package is.hail.expr.ir.agg

import is.hail.annotations.{Region, RegionUtils, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir._
import is.hail.expr.types.physical._
import is.hail.io.{CodecSpec, InputBuffer, OutputBuffer}
import is.hail.utils._
import is.hail.asm4s.coerce

trait AggregatorState {
  def fb: EmitFunctionBuilder[_]

  def storageType: PType

  def regionSize: Int = Region.TINY

  def createState: Code[Unit]
  def newState: Code[Unit]
  def initFromOffset(off: Code[Long]): Code[Unit] = newState

  def load(regionLoader: Code[Region] => Code[Unit], src: Code[Long]): Code[Unit]
  def store(regionStorer: Code[Region] => Code[Unit], dest: Code[Long]): Code[Unit]

  def copyFrom(src: Code[Long]): Code[Unit]

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit]

  def deserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit]
}

trait PointerBasedRVAState extends AggregatorState {
  private val r: ClassFieldRef[Region] = fb.newField[Region]
  val off: ClassFieldRef[Long] = fb.newField[Long]
  val storageType: PType = PInt64(true)
  val region: Code[Region] = r.load()

  override val regionSize: Int = Region.TINIER

  def newState: Code[Unit] = region.getNewRegion(regionSize)

  def createState: Code[Unit] = region.isNull.mux(Code(r := Code.newInstance[Region, Int](regionSize), region.invalidate()), Code._empty)

  def load(regionLoader: Code[Region] => Code[Unit], src: Code[Long]): Code[Unit] =
    Code(regionLoader(r), off := Region.loadAddress(src))

  def store(regionStorer: Code[Region] => Code[Unit], dest: Code[Long]): Code[Unit] =
    region.isValid.mux(Code(regionStorer(region), region.invalidate(), Region.storeAddress(dest, off)), Code._empty)

  def copyFrom(src: Code[Long]): Code[Unit] = copyFromAddress(Region.loadAddress(src))

  def copyFromAddress(src: Code[Long]): Code[Unit]
}

class TypedRVAState(val valueType: PType, val fb: EmitFunctionBuilder[_]) extends PointerBasedRVAState {
  override def load(regionLoader: Code[Region] => Code[Unit], src: Code[Long]): Code[Unit] =
    super.load(r => r.invalidate(), src)

  def copyFromAddress(src: Code[Long]): Code[Unit] = off := StagedRegionValueBuilder.deepCopy(fb, region, valueType, src)

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit] = {
    val enc = codec.buildEmitEncoderF[Long](valueType, valueType, fb)
    ob: Code[OutputBuffer] => enc(region, off, ob)
  }

  def deserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit] = {
    val dec = codec.buildEmitDecoderF[Long](valueType, valueType, fb)
    ib: Code[InputBuffer] => off := dec(region, ib)
  }
}

class TypedRVAState2(val valueType: PType, val fb: EmitFunctionBuilder[_]) extends AggregatorState {
  val storageType: PTuple = PTuple(required = true, valueType)
  private val needsRegion = valueType.containsPointers
  private val r: ClassFieldRef[Region] = fb.newField[Region]
  val off: ClassFieldRef[Long] = fb.newField[Long]

  def regionCodeOrElse[T](c: => Code[T], other: => Code[T]): Code[T] =
    if (needsRegion) c else other

  def regionCodeOrEmpty(c: => Code[Unit]): Code[Unit] = regionCodeOrElse(c, Code._empty)

  def newState: Code[Unit] = regionCodeOrEmpty(r.load().getNewRegion(regionSize))
  override def initFromOffset(src: Code[Long]): Code[Unit] = Code(newState, off := src)
  def createState: Code[Unit] = r.load().isNull.mux(Code(r := Code.newInstance[Region, Int](regionSize), r.load().invalidate()), Code._empty)

  def load(regionLoader: Code[Region] => Code[Unit], src: Code[Long]): Code[Unit] =
    Code(regionCodeOrEmpty(r.load().invalidate()), off := src)

  def store(regionStorer: Code[Region] => Code[Unit], dest: Code[Long]): Code[Unit] =
    Code(
      regionCodeOrEmpty(r.load().isValid.orEmpty(Code(regionStorer(r.load()), r.load().invalidate()))),
      dest.cne(off).orEmpty(Region.copyFrom(off, dest, storageType.byteSize)))

  def setMissing(): Code[Unit] = storageType.setFieldMissing(off, 0)

  def setValue(v: Code[_]): Code[Unit] = Code(
    newState,
    regionCodeOrElse(
      StagedRegionValueBuilder.deepCopy(fb, r, valueType, v, storageType.fieldOffset(off, 0)),
      Region.storeIRIntermediate(valueType)(storageType.fieldOffset(off, 0), v)),
    storageType.setFieldPresent(off, 0))

  def isMissing(): Code[Boolean] = storageType.isFieldMissing(off, 0)

  def value(): Code[_] = Region.loadIRIntermediate(valueType)(storageType.fieldOffset(off, 0))

  def copyFrom(src: Code[Long]): Code[Unit] =
    Code(
      newState,
      regionCodeOrElse(
        StagedRegionValueBuilder.deepCopy(fb, r.load(), storageType, src, off),
        Region.copyFrom(src, off, storageType.byteSize)
    )
  )

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit] = {
    val enc = codec.buildEmitEncoderF[Long](storageType, storageType, fb);
    ob: Code[OutputBuffer] => enc(r.load(), off, ob)
  }

  def deserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit] = {
    val dec = codec.buildEmitDecoderF[Long](storageType, storageType, fb);
    { ib: Code[InputBuffer] =>

      if (needsRegion)
        off := dec(r.load(), ib)
      else {
        val tmp = fb.newField[Long]
        Code(
          r.load().getNewRegion(regionSize),
          tmp := dec(r.load(), ib),
          copyFrom(tmp),
          r.load().invalidate())
      }
    }
  }
}

class PrimitiveRVAState(val types: Array[PType], val fb: EmitFunctionBuilder[_]) extends AggregatorState {
  type ValueField = (Option[ClassFieldRef[Boolean]], ClassFieldRef[_], PType)
  assert(types.forall(_.isPrimitive))

  val nFields: Int = types.length
  val fields: Array[ValueField] = Array.tabulate(nFields) { i =>
    val m = if (types(i).required) None else Some(fb.newField[Boolean](s"primitiveRVA_${i}_m"))
    val v = fb.newField(s"primitiveRVA_${i}_v")(typeToTypeInfo(types(i)))
    (m, v, types(i))
  }
  val storageType: PTuple = PTuple(types: _*)

  def foreachField(f: (Int, ValueField) => Code[Unit]): Code[Unit] =
    coerce[Unit](Code(Array.tabulate(nFields)(i => f(i, fields(i))) :_*))

  def newState: Code[Unit] = Code._empty
  def createState: Code[Unit] = Code._empty

  private[this] def loadVarsFromRegion(src: Code[Long]): Code[Unit] =
    foreachField {
      case (i, (None, v, t)) =>
        v.storeAny(Region.loadPrimitive(t)(storageType.fieldOffset(src, i)))
      case (i, (Some(m), v, t)) => Code(
        m := storageType.isFieldMissing(src, i),
        m.mux(Code._empty,
          v.storeAny(Region.loadPrimitive(t)(storageType.fieldOffset(src, i)))))
    }

  def load(regionLoader: Code[Region] => Code[Unit], src: Code[Long]): Code[Unit] =
    loadVarsFromRegion(src)

  def store(regionStorer: Code[Region] => Code[Unit], dest: Code[Long]): Code[Unit] =
      foreachField {
        case (i, (None, v, t)) =>
          Region.storePrimitive(t, storageType.fieldOffset(dest, i))(v)
        case (i, (Some(m), v, t)) =>
          m.mux(storageType.setFieldMissing(dest, i),
            Code(storageType.setFieldPresent(dest, i),
              Region.storePrimitive(t, storageType.fieldOffset(dest, i))(v)))
      }

  def copyFrom(src: Code[Long]): Code[Unit] = loadVarsFromRegion(src)

  def serialize(codec: CodecSpec): Code[OutputBuffer] => Code[Unit] = {
    ob: Code[OutputBuffer] =>
      foreachField {
        case (_, (None, v, t)) => ob.writePrimitive(t)(v)
        case (_, (Some(m), v, t)) => Code(
          ob.writeBoolean(m),
          m.mux(Code._empty, ob.writePrimitive(t)(v)))
      }
  }

  def deserialize(codec: CodecSpec): Code[InputBuffer] => Code[Unit] = {
    ib: Code[InputBuffer] =>
      foreachField {
        case (_, (None, v, t)) =>
          v.storeAny(ib.readPrimitive(t))
        case (_, (Some(m), v, t)) => Code(
          m := ib.readBoolean(),
          m.mux(Code._empty, v.storeAny(ib.readPrimitive(t))))
      }
  }
}

case class StateContainer(states: Array[AggregatorState], topRegion: Code[Region]) {
  val nStates: Int = states.length
  val typ: PTuple = PTuple(true, states.map { s => s.storageType }: _*)

  def apply(i: Int): AggregatorState = states(i)
  def getRegion(rOffset: Code[Int], i: Int): Code[Region] => Code[Unit] = { r: Code[Region] =>
    r.setFromParentReference(topRegion, rOffset + i, states(i).regionSize) }
  def getStateOffset(off: Code[Long], i: Int): Code[Long] = typ.fieldOffset(off, i)

  def setAllMissing(off: Code[Long]): Code[Unit] = toCode((i, _) =>
    topRegion.storeAddress(typ.fieldOffset(off, i), 0L))

  def toCode(f: (Int, AggregatorState) => Code[Unit]): Code[Unit] =
    coerce[Unit](Code(Array.tabulate(nStates)(i => f(i, states(i))): _*))

  def createStates: Code[Unit] =
    toCode((i, s) => s.createState)

  def initStates(stateOffset: Code[Long]): Code[Unit] =
    toCode((i, s) => s.initFromOffset(getStateOffset(stateOffset, i)))

  def load(rOffset: Code[Int], stateOffset: Code[Long]): Code[Unit] =
    toCode((i, s) => s.load(getRegion(rOffset, i), getStateOffset(stateOffset, i)))

  def store(rOffset: Code[Int], statesOffset: Code[Long]): Code[Unit] =
    toCode((i, s) => s.store(r => topRegion.setParentReference(r, rOffset + i), getStateOffset(statesOffset, i)))
}