package is.hail.annotations

import is.hail.asm4s._
import is.hail.asm4s.Code._
import is.hail.expr._
import is.hail.utils._

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

    var c = codeDec.invoke[MemoryBuffer, Long, Int, Unit]("readBytes", region.get, srvb.startOffset, (t.size + 7) / 8)
    var i = 0
    while (i < t.size) {
      c = Code(c,
        region.loadBit(srvb.startOffset,const(i).toL).mux(
          srvb.advance(),
          t.fieldType(i) match {
            case t2: TStruct =>
              val ssb = srvb.newStruct(t2)
              Code(
                srvb.startStruct(ssb),
                storeStruct(ssb),
                srvb.endStruct()
              )
            case t2: TArray =>
              val sab = srvb.newArray(t2)
              val length: LocalRef[Int] = fb.newLocal[Int]
              Code(
                length := codeDec.invoke[Int]("readInt"),
                srvb.startArray(sab, length),
                storeArray(sab, length),
                srvb.endArray()
              )
            case _ => storeType(t.fieldType(i), srvb)
          }
        )
      )
      i += 1
    }
    c
  }

  private def storeArray(srvb: StagedArrayBuilder[Decoder], length: Code[Int]): Code[Unit] = {

    val t = srvb.getType
    val fb = srvb.getFunctionBuilder

    val codeDec: LocalRef[Decoder] = srvb.input
    val region: StagedMemoryBuffer = srvb.region

    val c = codeDec.invoke[MemoryBuffer, Long, Int, Unit]("readBytes", region.get, srvb.startOffset + 4L, (length + 7) / 8)
    val d = t.elementType match {
      case t2: TArray =>
        val sab = new StagedArrayBuilder[Decoder](fb, t2)
        val length: LocalRef[Int] = sab.getFunctionBuilder.newLocal[Int]
        whileLoop(srvb.i < length,
          region.loadBit(srvb.startOffset + 4L, srvb.i.toL).mux(
            srvb.advance(),
            Code(
              length := codeDec.invoke[Int]("readInt"),
              srvb.startArray(sab, length),
              storeArray(sab, length),
              srvb.endArray()
            )
          )
        )
      case t2: TStruct =>
        val ssb = new StagedStructBuilder[Decoder](fb, t2)
        whileLoop(srvb.i < length,
          region.loadBit(srvb.startOffset + 4L, srvb.i.toL).mux(
            srvb.advance(),
            Code(
              srvb.startStruct(ssb),
              storeStruct(ssb),
              srvb.endStruct()
            )
          )
        )
      case _ =>
        whileLoop(srvb.i < length,
          region.loadBit(srvb.startOffset + 4L, srvb.i.toL).mux(
            srvb.advance(),
            storeType(t.elementType, srvb)
          )
        )
    }
    Code(c,d)
  }

  def getArrayReader(t: TArray, region: MemoryBuffer): (MemoryBuffer => Long) = {

    val srvb = new StagedArrayBuilder[Decoder](FunctionBuilder.functionBuilder[Decoder, MemoryBuffer, Long], t)
    val length: LocalRef[Int] = srvb.getFunctionBuilder.newLocal[Int]

    srvb.emit(
      Code(
        length := srvb.input.invoke[Int]("readInt"),
        srvb.start(length),
        storeArray(srvb, length)
      )
    )

    srvb.build()
    (r: MemoryBuffer) => srvb.transform(dec, r)
  }

  def getStructReader(t: TStruct, region: MemoryBuffer): (MemoryBuffer => Long) = {

    val srvb = new StagedStructBuilder[Decoder](FunctionBuilder.functionBuilder[Decoder, MemoryBuffer, Long], t)

    srvb.emit(
      Code(
        srvb.start(),
        storeStruct(srvb)
      )
    )

    srvb.build()
    (r: MemoryBuffer) => srvb.transform(dec, r)
  }

}
