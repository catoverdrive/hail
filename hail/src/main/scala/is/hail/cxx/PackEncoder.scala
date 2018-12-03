package is.hail.cxx

import is.hail.expr.types.physical._
import is.hail.io.{BufferSpec, NativeEncoderModule}

object PackEncoder {

  def encodeBinary(tub: TranslationUnitBuilder, output_buf_ptr: Expression, off: Expression): Code = {
    val len = tub.variable("len", "int", s"load_length($off)")
    s"""
       |${ len.define }
       |$output_buf_ptr->write_int($len);
       |$output_buf_ptr->write_bytes($off + 4, $len);""".stripMargin
  }

  def encodeArray(tub: TranslationUnitBuilder, t: PArray, output_buf_ptr: Expression, off: Expression): Code = {
    val len = tub.variable("len", "int", s"load_length($off)")
    val i = tub.variable("i", "int", s"0")
    val copyLengthAndMissing = if (t.elementType.required)
      s"$output_buf_ptr->write_int($len);"
    else
      s"""
         |$output_buf_ptr->write_int($len);
         |$output_buf_ptr->write_bytes($off + 4, n_missing_bytes($len));""".stripMargin
    val eltOff = tub.variable("eoff",
      "char *",
      s"round_up_alignment(${ if (!t.elementType.required) s"$off + 4 + n_missing_bytes($len)" else s"$off + 4" }, ${ t.elementType.alignment })")
    val elt = t.elementType match {
      case (_: PBinary | _: PArray) => s"load_address($eltOff)"
      case _ => eltOff.toString
    }

    val writeElt = if (t.elementType.required)
      encode(tub,  t.elementType, output_buf_ptr, Expression(elt))
    else
      s"""
         |if (!load_bit($off + 4, $i)) {
         |  ${ encode(tub, t.elementType, output_buf_ptr, Expression(elt)) };
         |}""".stripMargin

    s"""
       |${ len.define }
       |$copyLengthAndMissing
       |${ eltOff.define }
       |for (${ i.define } $i < $len; $i++) {
       |  $writeElt
       |  $eltOff += ${ t.elementByteSize };
       |}
      """.stripMargin
  }

  def encodeBaseStruct(tub: TranslationUnitBuilder, t: PBaseStruct, output_buf_ptr: Expression, off: Expression): Code = {
    val nMissingBytes = t.nMissingBytes
    val storeFields: Array[Code] = Array.tabulate[Code](t.size) { idx =>
      val store = t.types(idx) match {
        case t2@(_: PArray | _: PBinary) =>
          encode(tub, t2, output_buf_ptr, Expression(s"load_address($off + ${ t.byteOffsets(idx) })"))
        case t2 =>
          encode(tub, t2, output_buf_ptr, Expression(s"$off + ${ t.byteOffsets(idx) }"))

      }
      if (t.fieldRequired(idx)) {
        store
      } else {
        s"""
           |if (!load_bit($off, ${ t.missingIdx(idx) })) {
           |  $store
           |}""".stripMargin
      }
    }
    s"""
       |$output_buf_ptr->write_bytes($off, $nMissingBytes);
       |${ storeFields.mkString("\n") }
      """.stripMargin
  }

  def encode(tub: TranslationUnitBuilder, t: PType, output_buf_ptr: Expression, off: Expression): Code = t match {
    case _: PBoolean => s"$output_buf_ptr->write_byte(*($off) ? 1 : 0);"
    case _: PInt32 => s"$output_buf_ptr->write_int(load_int($off));"
    case _: PInt64 => s"$output_buf_ptr->write_long(load_long($off));"
    case _: PFloat32 => s"$output_buf_ptr->write_float(load_float($off));"
    case _: PFloat64 => s"$output_buf_ptr->write_double(load_double($off));"
    case _: PBinary => encodeBinary(tub, output_buf_ptr, off)
    case t2: PArray => encodeArray(tub, t2, output_buf_ptr, off)
    case t2: PBaseStruct => encodeBaseStruct(tub, t2, output_buf_ptr, off)
  }

  def apply(t: PType, bufSpec: BufferSpec, tub: TranslationUnitBuilder): Class = {
    assert(t.isInstanceOf[PBaseStruct] || t.isInstanceOf[PArray])
    tub.include("hail/hail.h")
    tub.include("hail/Encoder.h")
    tub.include("hail/ObjectArray.h")
    tub.include("hail/Utils.h")
    tub.include("<cstdio>")
    tub.include("<memory>")

    val encBuilder = tub.buildClass("Encoder", "NativeObj")

    val bufType = bufSpec.nativeOutputBufferType
    val buf = encBuilder.variable("buf", s"std::shared_ptr<$bufType>")
    encBuilder += buf

    encBuilder += s"${ encBuilder.name }(std::shared_ptr<OutputStream> os) : $buf(std::make_shared<$bufType>(os)) { }"

    val rowFB = encBuilder.buildMethod("encode_row", Array("const char *" -> "row"), "void")
    rowFB += encode(tub, t.fundamentalType, buf.ref, rowFB.getArg(0).ref)
    rowFB += "return;"
    rowFB.end()

    val byteFB = encBuilder.buildMethod("encode_byte", Array("char" -> "b"), "void")
    byteFB += s"$buf->write_byte(${ byteFB.getArg(0) });"
    byteFB += "return;"
    byteFB.end()

    val flushFB = encBuilder.buildMethod("flush", Array(), "void")
    flushFB += s"$buf->flush();"
    flushFB += "return;"
    flushFB.end()

    val closeFB = encBuilder.buildMethod("close", Array(), "void")
    closeFB +=
      s"""
         |$buf->close();
         |return;""".stripMargin
    closeFB.end()

    encBuilder.end()
  }

  def buildModule(t: PType, bufSpec: BufferSpec): NativeEncoderModule = {
    assert(t.isInstanceOf[PBaseStruct] || t.isInstanceOf[PArray])
    val tub = new TranslationUnitBuilder()

    val encClass = apply(t, bufSpec, tub)

    val outBufFB = tub.buildFunction("makeOutputBuffer", Array("NativeStatus*" -> "st", "long" -> "objects"), "NativeObjPtr")
    outBufFB += "UpcallEnv up;"
    outBufFB += s"auto joutput_stream = reinterpret_cast<ObjectArray*>(${ outBufFB.getArg(1) })->at(0);"
    val bufType = bufSpec.nativeOutputBufferType
    outBufFB += s"return std::make_shared<$encClass>(std::make_shared<OutputStream>(up, joutput_stream));"
    outBufFB.end()

    val rowFB = tub.buildFunction("encode_row", Array("NativeStatus*" -> "st", "long" -> "buf", "long" -> "row"), "long")
    rowFB += s"reinterpret_cast<$encClass *>(${ rowFB.getArg(1) })->encode_row(reinterpret_cast<char *>(${ rowFB.getArg(2) }));"
    rowFB += "return 0;"
    rowFB.end()

    val byteFB = tub.buildFunction("encode_byte", Array("NativeStatus*" -> "st", "long" -> "buf", "long" -> "b"), "long")
    byteFB += s"reinterpret_cast<$encClass *>(${ byteFB.getArg(1) })->encode_byte(${ byteFB.getArg(2) } & 0xff);"
    byteFB += "return 0;"
    byteFB.end()

    val flushFB = tub.buildFunction("encoder_flush", Array("NativeStatus*" -> "st", "long" -> "buf"), "long")
    flushFB += s"reinterpret_cast<$encClass *>(${ flushFB.getArg(1) })->flush();"
    flushFB += "return 0;"
    flushFB.end()

    val closeFB = tub.buildFunction("encoder_close", Array("NativeStatus*" -> "st", "long" -> "buf"), "long")
    closeFB += s"reinterpret_cast<$encClass *>(${ closeFB.getArg(1) })->close();"
    closeFB += "return 0;"
    closeFB.end()

    val mod = tub.end().build("-O1")

    NativeEncoderModule(mod.getKey, mod.getBinary)
  }
}
