package is.hail.annotations

import is.hail.asm4s.{AsmFunction2, Code, FunctionBuilder}
import is.hail.asm4s.Code._
import is.hail.asm4s._
import is.hail.expr._
import is.hail.utils._

class StagedStructBuilder[T](fb: FunctionBuilder[AsmFunction2[T,MemoryBuffer,Long]], rowType: TStruct)(implicit tti: TypeInfo[T]) extends StagedRegionValueBuilder[T](fb) {

  val alignment: Long = rowType.alignment

  val fieldOffsets: LocalRef[Array[Long]] = fb.newLocal[Array[Long]]
  val idx: LocalRef[Int] = fb.newLocal[Int]
  def currentOffset: Code[Long] = fieldOffsets.load()(idx.load())

  def getType: TStruct = rowType
  override def getCurrentOffset: Code[Long] = currentOffset

  def start(offset: Code[Long] = null, init: Boolean = true): Code[Unit] = {
    var c = startOffset.store(offset)

    if (offset == null)
      c = Code(
        region.align(rowType.alignment),
        startOffset.store(region.allocate(rowType.byteSize))
      )
    if (init)
      c = Code(c,rowType.clearMissingBits(region, startOffset.load()))
    c = Code(c,
        idx.store(0),
        fieldOffsets.store(Code.newArray[Long](rowType.size))
      )
    for (i <- 0 until rowType.size) {
      c = Code(c, fieldOffsets.load().update(i, startOffset.load() + rowType.byteOffsets(i)))
    }
    c
  }

  override def advance(): Code[Unit] = {
    idx.store(idx.load() + 1)
  }

  def setMissing(): Code[Unit] = {
    Code(
      rowType.setFieldMissing(region, startOffset, idx),
      advance()
    )
  }

  override def startArray(srvb: StagedArrayBuilder[T], length: Code[Int]): Code[Unit] = {
    Code(
      region.align(srvb.alignment),
      region.storeAddress(currentOffset, region.offset),
      srvb.start(length)
    )
  }

  override def endArray(): Code[Unit] = {
    advance()
  }

  override def startStruct(srvb: StagedStructBuilder[T]): Code[Unit] = {
//    Code(
//      region.align(srvb.alignment),
//      region.storeAddress(currentOffset, region.offset),
    srvb.start(currentOffset)
  }

  override def endStruct(): Code[Unit] = {
    advance()
  }

  override def addInt32(v: Code[Int]): Code[Unit] = {
    Code(
      region.storeInt32(currentOffset, v),
      advance()
    )
  }

  override def addBinary(bytes: Code[Array[Byte]]): Code[Unit] = {
    Code(
      region.align(TBinary.contentAlignment),
      region.storeAddress(currentOffset, region.offset),
      super.addBinary(bytes),
      advance()
    )
  }

  override def addString(str: Code[String]): Code[Unit] = {addBinary(str.invoke[Array[Byte]]("getBytes"))}


}

class StagedArrayBuilder[T](fb: FunctionBuilder[AsmFunction2[T,MemoryBuffer,Long]], rowType: TArray)(implicit tti: TypeInfo[T]) extends StagedRegionValueBuilder[T](fb) {

  val alignment: Long = rowType.contentsAlignment

  val currentOffset: LocalRef[Long] = fb.newLocal[Long]
  val nMissingBytes: LocalRef[Int] = fb.newLocal[Int]
  val i: LocalRef[Int] = fb.newLocal[Int]

  override def getCurrentOffset: Code[Long] = currentOffset
  def getType: TArray = rowType

  def start(length: Code[Int], init: Boolean = true): Code[Unit] = {
    var c = Code(
      region.align(rowType.contentsAlignment),
      startOffset.store(region.allocate(rowType.contentsByteSize(length))),
      currentOffset.store(startOffset.load() + rowType.elementsOffset(length))
    )
    if (init)
      c = Code(c, rowType.initialize(region, startOffset.load(), length, nMissingBytes, i))
    Code(c, i.store(0))
  }

  override def advance(): Code[Unit] = {
    Code(
      currentOffset.store(currentOffset.load() + rowType.elementByteSize),
      i.store(i.load() + 1)
    )
  }

  def setMissing(): Code[Unit] = {
    Code(
      rowType.setElementMissing(region, startOffset, i),
      advance()
    )
  }

  override def newArray(t: TArray = rowType.elementType.asInstanceOf[TArray]): StagedArrayBuilder[T] = {
    assert(rowType.elementType.isInstanceOf[TArray])
    new StagedArrayBuilder(fb, t)
  }

  override def newStruct(t: TStruct = rowType.elementType.asInstanceOf[TStruct]): StagedStructBuilder[T] = {
    assert(rowType.elementType.isInstanceOf[TStruct])
    new StagedStructBuilder(fb, t)
  }

  override def startArray(srvb: StagedArrayBuilder[T], length: Code[Int]): Code[Unit] = {
    assert(rowType.elementType.isInstanceOf[TArray])
    Code(
      region.align(srvb.alignment),
      region.storeAddress(currentOffset, region.offset),
      srvb.start(length)
    )
  }

  override def endArray(): Code[Unit] = {
    assert(rowType.elementType.isInstanceOf[TArray])
    advance()
  }

  override def startStruct(srvb: StagedStructBuilder[T]): Code[Unit] = {
    assert(rowType.elementType.isInstanceOf[TStruct])
    srvb.start(currentOffset)
  }

  override def endStruct(): Code[Unit] = {
    assert(rowType.elementType.isInstanceOf[TStruct])
    advance()
  }

  override def addInt32(v: Code[Int]): Code[Unit] = {
    assert(rowType.elementType == TInt32)
    Code(
      region.storeInt32(currentOffset, v),
      advance()
    )
  }

  override def addBinary(bytes: Code[Array[Byte]]): Code[Unit] = {
    assert(rowType.elementType.fundamentalType == TBinary)
    Code(
      region.align(TBinary.contentAlignment),
      region.storeAddress(currentOffset, region.offset),
      super.addBinary(bytes),
      advance()
    )
  }

  override def addString(str: Code[String]): Code[Unit] = {addBinary(str.invoke[Array[Byte]]("getBytes"))}
}

class StagedObjectBuilder[T](fb: FunctionBuilder[AsmFunction2[T, MemoryBuffer, Long]], t: Type)(implicit tti: TypeInfo[T]) extends StagedRegionValueBuilder[T](fb){

  override def newArray(t: TArray): StagedArrayBuilder[T] = {
    assert(false)
    new StagedArrayBuilder(fb, t)
  }

  override def startArray(srvb: StagedArrayBuilder[T], length: Code[Int]): Code[Unit] = {
    assert(false)
    _empty
  }

  override def endArray(): Code[Unit] = {
    assert(false)
    _empty
  }

  override def newStruct(t: TStruct): StagedStructBuilder[T] = {
    assert(false)
    new StagedStructBuilder(fb, t)
  }

  override def startStruct(srvb: StagedStructBuilder[T]): Code[Unit] = {
    assert(false)
    _empty
  }

  override def endStruct(): Code[Unit] = {
    assert(false)
    _empty
  }

}

class StagedRegionValueBuilder[T](fb: FunctionBuilder[AsmFunction2[T, MemoryBuffer, Long]])(implicit tti: TypeInfo[T]) {

  val input: LocalRef[T] = fb.getArg[T](1)
  val region: StagedMemoryBuffer = new StagedMemoryBuffer(fb.getArg[MemoryBuffer](2))
  val startOffset: LocalRef[Long] = fb.newLocal[Long]
  def getCurrentOffset: Code[Long] = region.size
  def getFunctionBuilder: FunctionBuilder[AsmFunction2[T, MemoryBuffer, Long]] = fb

  var transform: AsmFunction2[T, MemoryBuffer, Long] = _

  def start(t: Type): Code[Unit] = {
    assert(!t.isInstanceOf[TArray], "use StagedArrayBuilder for t of type TArray!")
    assert(!t.isInstanceOf[TStruct], "use StagedStructBuilder for t of type TStruct!")
    var c: Code[Unit] = _empty
    t.fundamentalType match {
      case TBinary =>
        c = Code(
          region.align(TBinary.contentAlignment),
          startOffset.store(region.offset)
        )
      case _ =>
        c = Code(region.align(t.alignment),
          startOffset.store(region.allocate(t.byteSize))
        )
    }
    c
  }

  def advance(): Code[Unit] = {
    assert(false)
    _empty
  }

  def build() {
//    fb.emit(getCode)
    fb.emit(_return(startOffset))
    transform = fb.result()()
  }

  def emit(c: Code[_]) {
    fb.emit(c)
  }

  def emit(cs: Array[Code[_]]) {
    for (c <- cs) {
      fb.emit(c)
    }
  }

  def newArray(t: TArray): StagedArrayBuilder[T] = {
    new StagedArrayBuilder(fb, t)
  }

  def startArray(srvb: StagedArrayBuilder[T], length: Code[Int]): Code[Unit] = {
    _empty
  }

  def endArray(): Code[Unit] = {
    _empty
  }

  def newStruct(t: TStruct): StagedStructBuilder[T] = {
    new StagedStructBuilder(fb, t)
  }

  def startStruct(srvb: StagedStructBuilder[T]) : Code[Unit] = {
    _empty
  }

  def endStruct() : Code[Unit] = {
    _empty
  }


  def addInt32(v: Code[Int]): Code[Unit] = {
    region.storeInt32(startOffset.load(), v)
  }

  def addBinary(bytes: Code[Array[Byte]]): Code[Unit] = {
    Code(
      region.appendInt32(bytes.length()),
      region.appendBytes(bytes)
    )
  }

  def addString(str: Code[String]): Code[Unit] = {
    addBinary(str.invoke[Array[Byte]]("getBytes"))
  }
}

class StagedMemoryBuffer(region: Code[MemoryBuffer]) {

  def get: Code[MemoryBuffer] = region

  def mem: Code[Long] = region.get[Long]("mem")

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