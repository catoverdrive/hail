package is.hail.expr.ir.functions

import is.hail.asm4s._
import is.hail.expr.types._
import is.hail.variant._

object CodeAllelePair {
  def apply(j: Code[Int], k: Code[Int]) = new CodeAllelePair(j | (k << 16))
}

class CodeAllelePair(p: Code[Int]) {
  val j: Code[Int] = p & 0xffff
  val k: Code[Int] = (p >> 16) & 0xffff
  val nNonRefAlleles: Code[Int] =
    j.ceq(0).mux(0, 1) + k.ceq(0).mux(0, 1)
  val alleleIndices: (Code[Int], Code[Int]) = (j, k)
}

object CallFunctions {
  def diploidGTIndex(j: Code[Int], k: Code[Int]): Code[Int] =
    k * (k + 1) / 2 + j

  def diploidGTIndexWithSwap(i: Code[Int], j: Code[Int]): Code[Int] = {
    (j < i).mux(diploidGTIndex(j, i), diploidGTIndex(i, j))
  }

  def call1(aj: Code[Int], phased: Code[Boolean]): Code[Call] =
    call(aj, phased, 1)

  def call2(aj: Code[Int], ak: Code[Int], phased: Code[Boolean]): Code[Call] =
    call(
      phased.mux(diploidGTIndex(aj, aj + ak),
        diploidGTIndexWithSwap(aj, ak)),
      phased, const(2))

  def call(ar: Code[Int], phased: Code[Boolean], ploidy: Code[Int]): Code[Call] = {
    const(0) | phased.toI | (ploidy & 3 << 1) | (ar << 3)
  }

  val throwBadPloidy = Code.fatal(s"invalid ploidy. Only support ploidy == 2")

  val ploidy: IRFunction[Int] = IRFunction[Int]("ploidy", TCall(), TInt32()) {
    case Array(c: Code[Call]) => (c >>> 1) & 0x3
  }

  val isPhased: IRFunction[Boolean] = IRFunction[Boolean]("isPhased", TCall(), TBoolean()) {
    case Array(call: Code[Call]) => (call & 0x1).ceq(1)
  }

  val isHaploid: IRFunction[Boolean] = IRFunction[Boolean]("isHaploid", TCall(), TBoolean()) {
    case Array(call: Code[Call]) => ploidy(call).ceq(1)
  }

  val isDiploid: IRFunction[Boolean] = IRFunction[Boolean]("isDiploid", TCall(), TBoolean()) {
    case Array(call: Code[Call]) => ploidy(call).ceq(2)
  }

  val isUnphasedDiploid: IRFunction[Boolean] = IRFunction[Boolean]("isUnphasedDiploid", TCall(), TBoolean()) {
    case Array(call: Code[Call]) => (call & 0x7).ceq(4)
  }

  val isPhasedDiploid: IRFunction[Boolean] = IRFunction[Boolean]("isPhasedDiploid", TCall(), TBoolean()) {
    case Array(call: Code[Call]) => (call & 0x7).ceq(5)
  }

  val alleleRepr: IRFunction[Int] = IRFunction[Int]("alleleRepr", TCall(), TInt32()) {
    case Array(call: Code[Call]) => call >>> 3
  }

  def allelePair(call: Code[Call]): CodeAllelePair = {
    new CodeAllelePair(
      isDiploid(call).mux(
        isPhased(call).mux(
          Code.invokeStatic[AllelePairFunctions, Int, Int]("allelePairFromPhased", alleleRepr(call)),
          Code.invokeStatic[AllelePairFunctions, Int, Int]("allelePair", alleleRepr(call))),
        throwBadPloidy))
  }

  val downcode: IRFunction[Call] = IRFunction[Call]("downcode", TCall(), TInt32(), TCall()) {
    case Array(call: Code[Call], i: Code[Int]) =>
      ploidy(call).ceq(2).mux(
        call2(allelePair(call).j.ceq(1).toI, allelePair(call).k.ceq(1).toI, isPhased(call)),
        ploidy(call).ceq(1).mux(
          call1(alleleByIndex(call, 0).ceq(i).toI, isPhased(call)),
          ploidy(call).ceq(0).mux(
            call,
            throwBadPloidy
          )
        )
      )
  }

  def alleleByIndex(c: Code[Call], i: Code[Int]): Code[Int] =
    ploidy(c).ceq(1).mux(
      alleleRepr(c),
      ploidy(c).ceq(2).mux(
        i.ceq(0).mux(allelePair(c).j, allelePair(c).k),
        throwBadPloidy
      )
    )
}