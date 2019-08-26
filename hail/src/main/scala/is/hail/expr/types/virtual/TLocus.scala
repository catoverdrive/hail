package is.hail.expr.types.virtual

import is.hail.annotations._
import is.hail.backend.BroadcastValue
import is.hail.check._
import is.hail.expr.types.physical.PLocus
import is.hail.utils._
import is.hail.variant._

import scala.reflect.{ClassTag, classTag}

object TLocus {
  def apply(rg: ReferenceGenome): TLocus = TLocus(rg.broadcastRG)

  def apply(rg: ReferenceGenome, required: Boolean): TLocus = TLocus(rg.broadcastRG, required)

  def representation(required: Boolean = false): TStruct = {
    TStruct(required,
      "contig" -> +TString(),
      "position" -> +TInt32())
  }

  def schemaFromRG(rg: Option[ReferenceGenome], required: Boolean = false): Type = rg match {
    case Some(ref) => TLocus(ref)
    case None => TLocus.representation(required)
  }
}

case class TLocus(rgBc: BroadcastValue[ReferenceGenome], override val required: Boolean = false) extends ComplexType {
  def rg: ReferenceGenome = rgBc.value

  lazy val physicalType: PLocus = PLocus(rgBc, required)

  def _toPretty = s"Locus($rg)"

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("locus<")
    sb.append(prettyIdentifier(rg.name))
    sb.append('>')
  }
  def _typeCheck(a: Any): Boolean = a.isInstanceOf[Locus]

  override def genNonmissingValue: Gen[Annotation] = Locus.gen(rg)

  override def scalaClassTag: ClassTag[Locus] = classTag[Locus]

  lazy val ordering: ExtendedOrdering =
    ExtendedOrdering.extendToNull(rg.locusOrdering)

  lazy val representation: TStruct = TLocus.representation(required)

  def locusOrdering: Ordering[Locus] = rg.locusOrdering

  override def unify(concrete: Type): Boolean = concrete match {
    case TLocus(crgBc, _) => rg == crgBc.value
    case _ => false
  }
}
