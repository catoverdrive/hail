package is.hail.expr.ir.functions

import java.io.Serializable

import is.hail.asm4s.Code
import is.hail.check.Gen
import is.hail.expr.Parser
import is.hail.expr.types.{TBoolean, TCall, TInt32}
import is.hail.utils.{fatal, triangle}
import is.hail.variant.Genotype.diploidGtIndex
import is.hail.variant._

import scala.annotation.switch

object CodeAllelePair {
  def apply(j: Code[Int], k: Code[Int]) = new CodeAllelePair(j | (k << 16))
}

Call2(if (p.j == i) 1 else 0, if (p.k == i) 1 else 0, Call.isPhased(c))

object Call extends Serializable {
  def apply(ar: Int, phased: Boolean, ploidy: Int): Call = {
    if (ploidy < 0 || ploidy > 2)
      fatal(s"invalid ploidy: $ploidy. Only support ploidy in range [0, 2]")
    if (ar < 0)
      fatal(s"invalid allele representation: $ar. Must be positive.")

    var c = 0
    c |= phased.toInt

    if (ploidy > 2)
      c |= (3 << 1)
    else
      c |= (ploidy << 1)

    if ((ar >>> 29) != 0)
      fatal(s"invalid allele representation: $ar. Max value is 2^29 - 1")

    c |= ar << 3
    c
  }

class CodeAllelePair(p: Code[Int]) {
  val j: Code[Int] = p & 0xffff
  val k: Code[Int] = (p >> 16) & 0xffff
  val nNonRefAlleles: Code[Int] =
    j.ceq(0).mux(0, 1) + k.ceq(0).mux(0, 1)
  val alleleIndices: (Code[Int], Code[Int]) = (j, k)
}

object CallFunctions {

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
        allelePair(call).j,
        ploidy(call).ceq(1).mux(
          if ploidy = 1,
          ploidy(call).ceq(0).mux(
            call,
            throwBadPloidy
          )
        )
      )
  }
  //  val reference = { (c: Call, i: Int) =>
  //    (Call.ploidy(c): @switch) match {
  //      case 0 => c
  //      case 1 =>
  //        Call1(if (Call.alleleByIndex(c, 0) == i) 1 else 0, Call.isPhased(c))
  //      case 2 =>
  //        val p = Call.allelePair(c)
  //        Call2(if (p.j == i) 1 else 0, if (p.k == i) 1 else 0, Call.isPhased(c))
  //      case _ =>
  //        CallN(Call.alleles(c).map(a => if (a == i) 1 else 0), Call.isPhased(c))
  //    }
  //  }

  def alleles(c: Call): Array[Int] = {
    (ploidy(c): @switch) match {
      case 0 => Array.empty[Int]
      case 1 => Array(alleleByIndex(c, 0))
      case 2 => allelePair(c).alleleIndices
      case _ => throw new UnsupportedOperationException
    }
  }

  def alleleByIndex(c: Call, i: Int): Int = {
    (ploidy(c): @switch) match {
      case 0 => throw new UnsupportedOperationException
      case 1 =>
        require(i == 0)
        alleleRepr(c)
      case 2 =>
        require (i == 0 || i == 1)
        val p = allelePair(c)
        if (i == 0) p.j else p.k
      case _ =>
        require(i >= 0 && i < ploidy(c))
        alleles(c)(i)
    }
  }

  def parse(s: String): Call = Parser.parseCall(s)

  def toString(c: Call): String = {
    val phased = isPhased(c)
    val sep = if (phased) "|" else "/"

    (ploidy(c): @switch) match {
      case 0 => if (phased) "|-" else "-"
      case 1 =>
        val a = alleleByIndex(c, 0)
        if (phased) s"|$a" else s"$a"
      case 2 =>
        val p = allelePair(c)
        s"${ p.j }$sep${ p.k }"
      case _ =>
        alleles(c).mkString(sep)
    }
  }

  def vcfString(c: Call, sb: StringBuilder): Unit = {
    val phased = isPhased(c)
    val sep = if (phased) "|" else "/"

    (ploidy(c): @switch) match {
      case 0 =>
        throw new UnsupportedOperationException("VCF spec does not support 0-ploid calls.")
      case 1 =>
        if (phased)
          throw new UnsupportedOperationException("VCF spec does not support phased haploid calls.")
        else
          sb.append(alleleByIndex(c, 0))
      case 2 =>
        val p = allelePair(c)
        sb.append(p.j)
        sb.append(sep)
        sb.append(p.k)
      case _ =>
        var i = 0
        val nAlleles = ploidy(c)
        while (i < nAlleles) {
          sb.append(alleleByIndex(c, i))
          if (i != nAlleles - 1)
            sb.append(sep)
          i += 1
        }
    }
  }

  def isHomRef(c: Call): Boolean = {
    (ploidy(c): @switch) match {
      case 0 => false
      case 1 | 2 => alleleRepr(c) == 0
      case _ => alleles(c).forall(_ == 0)
    }
  }

  def isHet(c: Call): Boolean = {
    (ploidy(c): @switch) match {
      case 0 | 1 => false
      case 2 => alleleRepr(c) > 0 && {
        val p = allelePair(c)
        p.j != p.k
      }
      case _ => throw new UnsupportedOperationException
    }
  }

  def isHomVar(c: Call): Boolean = {
    (ploidy(c): @switch) match {
      case 0 => false
      case 1 => alleleRepr(c) > 0
      case 2 => alleleRepr(c) > 0 && {
        val p = allelePair(c)
        p.j == p.k
      }
      case _ => throw new UnsupportedOperationException
    }
  }

  def isNonRef(c: Call): Boolean = {
    (ploidy(c): @switch) match {
      case 0 => false
      case 1 | 2 => alleleRepr(c) > 0
      case _ => alleles(c).exists(_ != 0)
    }
  }

  def isHetNonRef(c: Call): Boolean = {
    (ploidy(c): @switch) match {
      case 0 | 1 => false
      case 2 => alleleRepr(c) > 0 && {
        val p = allelePair(c)
        p.j > 0 && p.k > 0 && p.k != p.j
      }
      case _ => throw new UnsupportedOperationException
    }
  }

  def isHetRef(c: Call): Boolean = {
    (ploidy(c): @switch) match {
      case 0 | 1 => false
      case 2 => alleleRepr(c) > 0 && {
        val p = allelePair(c)
        (p.j == 0 && p.k > 0) || (p.k == 0 && p.j > 0)
      }
      case _ => throw new UnsupportedOperationException
    }
  }

  def nNonRefAlleles(c: Call): Int = {
    (ploidy(c): @switch) match {
      case 0 => 0
      case 1 => (alleleRepr(c) > 0).toInt
      case 2 => allelePair(c).nNonRefAlleles
      case _ => alleles(c).count(_ != 0)
    }
  }

  def oneHotAlleles(c: Call, nAlleles: Int): IndexedSeq[Int] = {
    var j = 0
    var k = 0

    if (ploidy(c) == 2) {
      val p = allelePair(c)
      j = p.j
      k = p.k
    }

    new IndexedSeq[Int] with Serializable {
      def length: Int = nAlleles

      def apply(idx: Int): Int = {
        if (idx < 0 || idx >= nAlleles)
          throw new ArrayIndexOutOfBoundsException(idx)

        (ploidy(c): @switch) match {
          case 0 => 0
          case 1 => (alleleRepr(c) == idx).toInt
          case 2 =>
            var r = 0
            if (idx == j)
              r += 1
            if (idx == k)
              r += 1
            r
          case _ => throw new UnsupportedOperationException
        }
      }
    }
  }

  def check(c: Call, nAlleles: Int) {
    (ploidy(c): @switch) match {
      case 0 =>
      case 1 =>
        val a = alleleByIndex(c, 0)
        assert(a >= 0 && a < nAlleles)
      case 2 =>
        val nGenotypes = triangle(nAlleles)
        val udtn =
          if (isPhased(c)) {
            val p = allelePair(c)
            unphasedDiploidGtIndex(Call2(p.j, p.k))
          } else
            unphasedDiploidGtIndex(c)
        assert(udtn < nGenotypes, s"Invalid call found `${ c.toString }' for number of alleles equal to `$nAlleles'.")
      case _ =>
        alleles(c).foreach(a => assert(a >= 0 && a < nAlleles))
    }
  }
}

object GenotypeFunctions {
  val smallAllelePair = Array(AllelePair(0, 0), AllelePair(0, 1), AllelePair(1, 1),
    AllelePair(0, 2), AllelePair(1, 2), AllelePair(2, 2),
    AllelePair(0, 3), AllelePair(1, 3), AllelePair(2, 3), AllelePair(3, 3),
    AllelePair(0, 4), AllelePair(1, 4), AllelePair(2, 4), AllelePair(3, 4), AllelePair(4, 4),
    AllelePair(0, 5), AllelePair(1, 5), AllelePair(2, 5), AllelePair(3, 5), AllelePair(4, 5), AllelePair(5, 5),
    AllelePair(0, 6), AllelePair(1, 6), AllelePair(2, 6), AllelePair(3, 6), AllelePair(4, 6), AllelePair(5, 6),
    AllelePair(6, 6),
    AllelePair(0, 7), AllelePair(1, 7), AllelePair(2, 7), AllelePair(3, 7), AllelePair(4, 7),
    AllelePair(5, 7), AllelePair(6, 7), AllelePair(7, 7))

  def allelePairRecursive(i: Int): AllelePair = {
    def f(j: Int, k: Int): AllelePair = if (j <= k)
      AllelePair(j, k)
    else
      f(j - k - 1, k + 1)

    f(i, 0)
  }

  def allelePairSqrt(i: Int): AllelePair = {
    val k: Int = (Math.sqrt(8 * i.toDouble + 1) / 2 - 0.5).toInt
    assert(k * (k + 1) / 2 <= i)
    val j = i - k * (k + 1) / 2
    assert(diploidGtIndex(j, k) == i)
    AllelePair(j, k)
  }

  def allelePair(i: Int): AllelePair = {
    if (i < smallAllelePair.length)
      smallAllelePair(i)
    else
      allelePairSqrt(i)
  }
}
