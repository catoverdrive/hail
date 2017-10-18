package is.hail.annotations

import is.hail.asm4s.{AsmFunction2, Code, FunctionBuilder}
import is.hail.asm4s.Code._
import is.hail.asm4s._
import is.hail.expr._
import is.hail.utils._
import scala.reflect.ClassTag

class StagedRegionValueBuilder[T] private (val fb: FunctionBuilder[AsmFunction2[T, MemoryBuffer, Long]], val rowType: Type, var region: Code[MemoryBuffer], var currentOffset: LocalRef[Long])(implicit tti: TypeInfo[T]) {

  private def this(fb: FunctionBuilder[AsmFunction2[T, MemoryBuffer, Long]], rowType: Type, parent: StagedRegionValueBuilder[T])(implicit tti: TypeInfo[T]) = {
    this(fb, rowType, parent.region, parent.currentOffset)
    this.parent = parent
  }
  def this(fb: FunctionBuilder[AsmFunction2[T, MemoryBuffer, Long]], rowType: Type)(implicit tti: TypeInfo[T]) = {
    this(fb, rowType, fb.getArg[MemoryBuffer](2), fb.newLocal[Long])
  }

  var parent: StagedRegionValueBuilder[T] = _

  val input: LocalRef[T] = fb.getArg[T](1)
  var idx: LocalRef[Int] = _
  var fieldOffsets: LocalRef[Array[Long]] = _
  var elementsOffset: LocalRef[Long] = _

  var transform: AsmFunction2[T, MemoryBuffer, Long] = _

  val startOffset: LocalRef[Long] = fb.newLocal[Long]
  val endOffset: Code[Long] = region.size

  def start(): Code[Unit] = {
    assert(!rowType.isInstanceOf[TArray])
    rowType.fundamentalType match {
      case _: TStruct => start(true)
      case TBinary =>
        assert (parent == null)
        _empty[Unit]
      case _ => Code(
        region.align(rowType.alignment),
        startOffset.store(region.allocate(rowType.byteSize)),
        currentOffset.store(startOffset)
      )
    }
  }

  def start(length: Code[Int], init: Boolean = true): Code[Unit] = {
    val t = rowType.asInstanceOf[TArray]
    idx = fb.newLocal[Int]
    elementsOffset = fb.newLocal[Long]
    var c = Code(
        region.align(t.contentsAlignment),
        startOffset.store(region.allocate(t.contentsByteSize(length)))
    )
    if (parent != null) {
      c = Code(c, region.storeAddress(currentOffset, startOffset))
    }
    c = Code(c, currentOffset.store(startOffset.load() + t.elementsOffset(length)))
    if (init)
      c = Code(c, t.initialize(region, startOffset.load(), length, idx))
    Code(c, idx.store(0))
  }

  def start(init: Boolean): Code[Unit] = {
    val t = rowType.asInstanceOf[TStruct]
    idx = fb.newLocal[Int]
    fieldOffsets = fb.newLocal[Array[Long]]
    var c = if (parent == null)
      Code(
        region.align(t.alignment),
        startOffset.store(region.allocate(t.byteSize)),
        currentOffset.store(startOffset)
      )
    else
      startOffset.store(currentOffset)
    if (init)
      c = Code(c,t.clearMissingBits(region, startOffset.load()))
    c = Code(c,
      idx.store(0),
      fieldOffsets.store(Code.newArray[Long](t.size))
    )
    for (i <- 0 until t.size) {
      c = Code(c, fieldOffsets.load().update(i, startOffset.load() + t.byteOffsets(i)))
    }
    c
  }

  def setMissing(): Code[Unit] = {
    Code(
      rowType match {
        case t: TArray => t.setElementMissing(region, startOffset, idx)
        case t: TStruct => t.setFieldMissing(region, startOffset, idx)
      },
      advance()
    )
  }

  def addInt32(v: Code[Int]): Code[Unit] = {
    Code(
      region.storeInt32(currentOffset, v),
      advance()
    )
  }

  def addBinary(bytes: Code[Array[Byte]]): Code[Unit] = {
    Code(
      region.align(TBinary.contentAlignment),
      if (rowType == TBinary) {
        Code(
          startOffset.store(endOffset),
          currentOffset.store(startOffset)
        )
      } else region.storeAddress(currentOffset,endOffset),
      region.appendInt32(bytes.length()),
      region.appendBytes(bytes),
      advance()
    )
  }

  def addString(str: Code[String]): Code[Unit] = addBinary(str.invoke[Array[Byte]]("getBytes"))

  def addArray(t: TArray, f: (StagedRegionValueBuilder[T] => Code[Unit])): Code[Unit] = Code(f(new StagedRegionValueBuilder[T](fb, t, this)), advance())

  def addStruct(t: TStruct, f: (StagedRegionValueBuilder[T] => Code[Unit])): Code[Unit] = Code(f(new StagedRegionValueBuilder[T](fb, t, this)), advance())

  private def advance(): Code[Unit] = {
    rowType match {
      case t: TArray => Code(
        idx.store(idx.load() + 1),
        currentOffset.store(elementsOffset.load() + (idx.toL * t.elementByteSize))
      )
      case t: TStruct => Code(
        idx.store(idx.load() + 1),
        currentOffset.store(fieldOffsets.load()(idx))
      )
      case _ => _empty[Unit]
    }
  }

  def build() {
    emit(_return(startOffset))
    transform = fb.result()()
  }

  def emit(c: Code[_]) {fb.emit(c)}

  def emit(cs: Array[Code[_]]) {
    for (c <- cs) {
      fb.emit(c)
    }
  }
}

class RichCodeMemoryBuffer(val region: Code[MemoryBuffer]) extends AnyVal {

  def get: Code[MemoryBuffer] = region

  def size: Code[Long] = region.invoke[Long]("size")

  def offset: Code[Long] = region.invoke[Long]("size")

  def storeInt32(off: Code[Long], v: Code[Int]): Code[Unit] = {
      region.invoke[Long,Int,Unit]("storeInt", off, v)
  }

  def storeInt64(off: Code[Long], v: Code[Long]): Code[Unit] = {
    region.invoke[Long,Long,Unit]("storeLong", off, v)
  }

  def storeFloat32(off: Code[Long], v: Code[Float]): Code[Unit] = {
    region.invoke[Long,Float,Unit]("storeFloat", off, v)
  }

  def storeFloat64(off: Code[Long], v: Code[Double]): Code[Unit] = {
    region.invoke[Long,Double,Unit]("storeDouble", off, v)
  }

  def storeAddress(off: Code[Long], a: Code[Long]): Code[Unit] = {
    region.invoke[Long,Long,Unit]("storeAddress", off, a)
  }

  def storeByte(off: Code[Long], b: Code[Byte]): Code[Unit] = {
    region.invoke[Long, Byte, Unit]("storeByte", off, b)
  }

  def storeBytes(off: Code[Long], bytes: Code[Array[Byte]]): Code[Unit] = {
    region.invoke[Long, Array[Byte], Unit]("storeBytes", off, bytes)
  }

  def align(alignment: Code[Long]): Code[Unit] = {
    region.invoke[Long, Unit]("align", alignment)
  }

  def allocate(n: Code[Long]): Code[Long] = {
    region.invoke[Long, Long]("allocate",n)
  }

  def alignAndAllocate(n: Code[Long]): Code[Long] = {
    region.invoke[Long, Long]("alignAndAllocate",n)
  }


  def loadBoolean(off: Code[Long]): Code[Boolean] = {
    region.invoke[Long, Boolean]("loadBoolean", off)
  }

  def loadBit(byteOff: Code[Long], bitOff: Code[Long]): Code[Boolean] = {
    region.invoke[Long, Long, Boolean]("loadBit", byteOff, bitOff)
  }

  def setBit(byteOff: Code[Long], bitOff: Code[Long]): Code[Unit] = {
    region.invoke[Long, Long, Unit]("setBit", byteOff, bitOff)
  }

  def clearBit(byteOff: Code[Long], bitOff: Code[Long]): Code[Unit] = {
    region.invoke[Long, Long, Unit]("clearBit", byteOff, bitOff)
  }

  def storeBit(byteOff: Code[Long], bitOff: Code[Long], b: Code[Boolean]): Code[Unit] = {
    region.invoke[Long, Long, Boolean, Unit]("setBit", byteOff, bitOff, b)
  }

  def appendInt32(i: Code[Int]): Code[Unit] = {
    region.invoke[Int, Unit]("appendInt", i)
  }

  def appendInt64(l: Code[Long]): Code[Unit] = {
    region.invoke[Long, Unit]("appendLong", l)
  }

  def appendFloat32(f: Code[Float]): Code[Unit] = {
    region.invoke[Float, Unit]("appendFloat", f)
  }

  def appendFloat64(d: Code[Double]): Code[Unit] = {
    region.invoke[Double, Unit]("appendDouble", d)
  }

  def appendByte(b: Code[Byte]): Code[Unit] = {
    region.invoke[Byte, Unit]("appendByte", b)
  }

  def appendBytes(bytes: Code[Array[Byte]]): Code[Unit] = {
    region.invoke[Array[Byte], Unit]("appendBytes", bytes)
  }

  def appendBytes(bytes: Code[Array[Byte]], bytesOff: Code[Long], n: Code[Int]): Code[Unit] = {
    region.invoke[Array[Byte],Long, Int, Unit]("appendBytes", bytes, bytesOff, n)
  }

  def clear(): Code[Unit] = {
    region.invoke[Unit]("clear")
  }

}