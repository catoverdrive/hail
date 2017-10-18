package is.hail.annotations

import is.hail.asm4s._
import is.hail.asm4s.Code._
import is.hail.expr._
import is.hail.utils._
import scala.language.implicitConversions

class StagedEncoder {

}

object StagedDecoder {
  implicit def toStagedDecoder(dec: Decoder): StagedDecoder = new StagedDecoder(dec)
}

final class StagedDecoder(val dec: Decoder) extends AnyVal {

  private def storeType(typ: Type, srvb: StagedRegionValueBuilder[Decoder]): Code[Unit] = {
    assert(!typ.isInstanceOf[TStruct])
    assert(!typ.isInstanceOf[TArray])
    typ.fundamentalType match {
      case TInt32 => srvb.addInt32(srvb.input.invoke[Int]("readInt"))
//      case TInt64 => srvb.addInt64(srvb.input.invoke[Int]("readLong"))
//      case TFloat32 => srvb.addFloat32(srvb.input.invoke[Int]("readFloat"))
//      case TFloat64 => srvb.addFloat64(srvb.input.invoke[Int]("readDouble"))
      case TBinary => Code(
        srvb.input.invoke[MemoryBuffer, Long, Unit]("readBinary", srvb.region.get, srvb.currentOffset),
        srvb.advance()
      )
    }
  }

  private def storeStruct(srvb: StagedRegionValueBuilder[Decoder]): Code[Unit] = {
    val t = srvb.rowType.asInstanceOf[TStruct]
    val fb = srvb.fb

    val codeDec: LocalRef[Decoder] = srvb.input
    val region: Code[MemoryBuffer] = srvb.region

    var c = Code(
      srvb.start(),
      codeDec.invoke[MemoryBuffer, Long, Int, Unit]("readBytes", region.get, srvb.startOffset, (t.size + 7) / 8)
    )
    var i = 0
    while (i < t.size) {
      c = Code(c,
        region.loadBit(srvb.startOffset,const(i).toL).mux(
          srvb.advance(),
          t.fieldType(i) match {
            case t2: TStruct => srvb.addStruct(t2, storeStruct)
            case t2: TArray =>
              val length: LocalRef[Int] = fb.newLocal[Int]
              Code(
                length := codeDec.invoke[Int]("readInt"),
                srvb.addArray(t2, sab => storeArray(sab, length))
              )
            case _ => storeType(t.fieldType(i), srvb)
          }
        )
      )
      i += 1
    }
    c
  }

  private def storeArray(srvb: StagedRegionValueBuilder[Decoder], length: LocalRef[Int]): Code[Unit] = {

    val t = srvb.rowType.asInstanceOf[TArray]
    val fb = srvb.fb

    val codeDec: LocalRef[Decoder] = srvb.input
    val region: Code[MemoryBuffer] = srvb.region

    val c = Code(
      srvb.start(length),
      codeDec.invoke[MemoryBuffer, Long, Int, Unit]("readBytes", region.get, srvb.startOffset + 4L, (length + 7) / 8)
    )
    val d = t.elementType match {
      case t2: TArray =>
        val sab = new StagedRegionValueBuilder[Decoder](fb, t2)
        val length: LocalRef[Int] = srvb.fb.newLocal[Int]
        whileLoop(srvb.idx < length,
          region.loadBit(srvb.startOffset + 4L, srvb.idx.toL).mux(
            srvb.advance(),
            Code(
              length := codeDec.invoke[Int]("readInt"),
              srvb.addArray(t2, sab => storeArray(sab, length))
            )
          )
        )
      case t2: TStruct =>
        val ssb = new StagedRegionValueBuilder[Decoder](fb, t2)
        whileLoop(srvb.idx < length,
          region.loadBit(srvb.startOffset + 4L, srvb.idx.toL).mux(
            srvb.advance(),
            srvb.addStruct(t2, storeStruct)
          )
        )
      case _ =>
        whileLoop(srvb.idx < length,
          region.loadBit(srvb.startOffset + 4L, srvb.idx.toL).mux(
            srvb.advance(),
            storeType(t.elementType, srvb)
          )
        )
    }
    Code(c,d)
  }

  def getArrayReader(t: TArray): (MemoryBuffer => Long) = {

    val srvb = new StagedRegionValueBuilder[Decoder](FunctionBuilder.functionBuilder[Decoder, MemoryBuffer, Long], t)
    val length: LocalRef[Int] = srvb.fb.newLocal[Int]

    srvb.emit(
      Code(
        length := srvb.input.invoke[Int]("readInt"),
        storeArray(srvb, length)
      )
    )
    srvb.build()
    (r: MemoryBuffer) => srvb.transform(dec, r)
  }

  def getStructReader(t: TStruct): (MemoryBuffer => Long) = {

    val srvb = new StagedRegionValueBuilder[Decoder](FunctionBuilder.functionBuilder[Decoder, MemoryBuffer, Long], t)

    srvb.emit(storeStruct(srvb))
    srvb.build()
    (r: MemoryBuffer) => srvb.transform(dec, r)
  }

  def getRegionValueReader(t: Type): (MemoryBuffer => Long) = {
    t.fundamentalType match {
      case t2: TArray => getArrayReader(t2)
      case t2: TStruct => getStructReader(t2)
    }
  }

}
