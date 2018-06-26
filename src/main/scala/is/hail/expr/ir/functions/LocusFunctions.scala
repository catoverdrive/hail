package is.hail.expr.ir.functions

import is.hail.annotations.{Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.types._
import is.hail.expr.types
import is.hail.asm4s
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.variant._
import is.hail.expr.ir._
import is.hail.utils._

object LocusFunctions extends RegistryFunctions {

  val trep = TLocus.representation()

  def registerAll() {
    registerCode("contig", tv("T", _.isInstanceOf[TLocus]), TString()) {
      case (mb, locus: Code[Long]) =>
        trep.loadField(getRegion(mb), locus, 0)
    }

    registerCode("position", tv("T", _.isInstanceOf[TLocus]), TInt32()) {
      case (mb, locus: Code[Long]) =>
        getRegion(mb).loadInt(trep.fieldOffset(locus, 1))
    }
  }
}

class LocusFunctions(rg: ReferenceGenome) extends RegistryFunctions {

  val tlocus = TLocus(rg)
  var registered: Set[String] = Set[String]()

  def getLocus(mb: EmitMethodBuilder, locus: Code[Long]): Code[Locus] = {
    Code.checkcast[Locus](wrapArg(mb, tlocus)(locus).asInstanceOf[Code[AnyRef]])
  }

  def registerName(mname: String): String = {
    registered += rg.wrapFunctionName(mname)
    rg.wrapFunctionName(mname)
  }

  def registerLocusCode(methodName: String): Unit = {
    registerCode(registerName(methodName), tlocus, TBoolean()) {
      case (mb: EmitMethodBuilder, locus: Code[Long]) =>
        val locusObject = getLocus(mb, locus)
        val tlocus = types.coerce[TLocus](tv("T").t)
        val rg = tlocus.rg.asInstanceOf[ReferenceGenome]

        val codeRG = mb.getReferenceGenome(rg)
        unwrapReturn(mb, TBoolean())(locusObject.invoke[RGBase, Boolean](methodName, codeRG))
    }
  }

  def registerAll() {
    registerLocusCode("isAutosomalOrPseudoAutosomal")
    registerLocusCode("isAutosomal")
    registerLocusCode("inYNonPar")
    registerLocusCode("inXPar")
    registerLocusCode("isMitochondrial")
    registerLocusCode("inXNonPar")
    registerLocusCode("inYPar")

    val locusAllelesType = TTuple(tlocus, TArray(TString()))
    registerCode(registerName("min_rep"), tlocus, TArray(TString()), locusAllelesType) { (mb, lOff, aOff) =>
      val returnTuple = mb.newLocal[(Locus, IndexedSeq[String])]
      val locus = getLocus(mb, lOff)
      val alleles = Code.checkcast[IndexedSeq[String]](wrapArg(mb, TArray(TString()))(aOff).asInstanceOf[Code[AnyRef]])
      val tuple = Code.invokeScalaObject[Locus, IndexedSeq[String], (Locus, IndexedSeq[String])](VariantMethods.getClass, "minRep", locus, alleles)

      val newLocus = Code.checkcast[Locus](returnTuple.load().get[java.lang.Object]("_1"))
      val newAlleles = Code.checkcast[IndexedSeq[String]](returnTuple.load().get[java.lang.Object]("_2"))

      val srvb = new StagedRegionValueBuilder(mb, locusAllelesType)
      Code(
        returnTuple := tuple,
        srvb.start(),
        srvb.addBaseStruct(types.coerce[TBaseStruct](tlocus.fundamentalType), { locusBuilder =>
          Code(
            locusBuilder.start(),
            locusBuilder.addString(newLocus.invoke[String]("contig")),
            locusBuilder.advance(),
            locusBuilder.addInt(newLocus.invoke[Int]("position")))
        }),
        srvb.advance(),
        srvb.addArray(TArray(TString()), { allelesBuilder =>
          Code(
            allelesBuilder.start(newAlleles.invoke[Int]("size")),
            Code.whileLoop(allelesBuilder.arrayIdx < newAlleles.invoke[Int]("size"),
              allelesBuilder.addString(Code.checkcast[String](newAlleles.invoke[Int, java.lang.Object]("apply", allelesBuilder.arrayIdx))),
              allelesBuilder.advance()))
        }),
        srvb.offset)
    }
  }
}
