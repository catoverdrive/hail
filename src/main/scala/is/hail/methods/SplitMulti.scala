package is.hail.methods

import is.hail.annotations._
import is.hail.asm4s.{AsmFunction13, FunctionBuilder}
import is.hail.expr.{BaseIR, _}
import is.hail.expr.ir._
import is.hail.expr.types._
import is.hail.rvd.OrderedRVD
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class ExprAnnotator(val ec: EvalContext, t: TStruct, expr: String, head: Option[String]) extends Serializable {
  private val (paths, types, f) = Parser.parseAnnotationExprs(expr, ec, head)

  private val inserters = new Array[Inserter](types.length)
  val newT: TStruct = {
    var newT = t
    var i = 0
    while (i < types.length) {
      val (newSig, ins) = newT.structInsert(types(i), paths(i))
      inserters(i) = ins
      newT = newSig
      i += 1
    }
    newT
  }

  def insert(a: Annotation): Annotation = {
    var newA = a
    var i = 0
    val xs = f()
    while (i < xs.length) {
      newA = inserters(i)(newA, xs(i))
      i += 1
    }
    newA
  }
}

class SplitMultiRowIR(vIR: Array[(String, IR)], entryIR: Array[(String, IR)], oldMatrixType: MatrixType) {
  val oldRowIR = Ref("va")
  val newEntries = ArrayMap(GetField(In(3, oldMatrixType.rvRowType), MatrixType.entriesIdentifier), "g", InsertFields(Ref("g"), entryIR))
  val changedFields: Seq[(String, IR)] =
    (vIR
      :+ ("locus", Ref("newLocus"))
      :+ ("alleles", Ref("newAlleles"))
      :+ (MatrixType.entriesIdentifier, newEntries))

  val newRowIR: IR = InsertFields(oldRowIR, changedFields)

  val (t, splitRow): (Type, () => AsmFunction13[Region, Long, Boolean, Long, Boolean, Long, Boolean, Long, Boolean, Int, Boolean, Boolean, Boolean, Long]) =
    Compile[Long, Long, Long, Long, Int, Boolean, Long](
      "global", RegionValueRep[Long](oldMatrixType.globalType),
      "newLocus", RegionValueRep[Long](oldMatrixType.rowType.fieldByName("locus").typ),
      "newAlleles", RegionValueRep[Long](oldMatrixType.rowType.fieldByName("alleles").typ),
      "va", RegionValueRep[Long](oldMatrixType.rvRowType),
      "aIndex", RegionValueRep[Int](TInt32()),
      "wasSplit", RegionValueRep[Boolean](TBoolean()),
      newRowIR)

  val newMatrixType: MatrixType = oldMatrixType.copy(rvRowType = coerce[TStruct](t))
}

class SplitMultiPartitionContextIR(
  keepStar: Boolean,
  nSamples: Int, globalAnnotation: Annotation, matrixType: MatrixType,
  rowF: () => AsmFunction13[Region, Long, Boolean, Long, Boolean, Long, Boolean, Long, Boolean, Int, Boolean, Boolean, Boolean, Long], newRVRowType: TStruct) extends
  SplitMultiPartitionContext(keepStar, nSamples, globalAnnotation, matrixType, newRVRowType) {

  private var globalsCopied = false
  private var globals: Long = 0
  private var globalsEnd: Long = 0

  private def copyGlobals() {
    splitRegion.clear()
    rvb.set(splitRegion)
    rvb.start(matrixType.globalType)
    rvb.addAnnotation(matrixType.globalType, globalAnnotation)
    globals = rvb.end()
    globalsEnd = splitRegion.size
    globalsCopied = true
  }

  private val allelesType = matrixType.rowType.fieldByName("alleles").typ
  private val locusType = matrixType.rowType.fieldByName("locus").typ
  val f = rowF()

  def constructSplitRow(splitVariants: Iterator[(Variant, Int)], rv: RegionValue, wasSplit: Boolean): Iterator[RegionValue] = {
    if (!globalsCopied)
      copyGlobals()
    splitRegion.clear(globalsEnd)
    rvb.start(matrixType.rvRowType)
    rvb.addRegionValue(matrixType.rvRowType, rv)
    val oldRow = rvb.end()
    val oldEnd = splitRegion.size
    splitVariants.map { case (sjv, aIndex) =>
      splitRegion.clear(oldEnd)

      rvb.start(locusType)
      rvb.addAnnotation(locusType, sjv.locus)
      val locusOff = rvb.end()

      rvb.start(allelesType)
      rvb.addAnnotation(allelesType, sjv.alleles)
      val allelesOff = rvb.end()

      val off = f(splitRegion, globals, false, locusOff, false, allelesOff, false, oldRow, false, aIndex, false, wasSplit, false)
      splitrv.set(splitRegion, off)
      splitrv
    }
  }
}

class SplitMultiPartitionContextAST(
  keepStar: Boolean,
  nSamples: Int, globalAnnotation: Annotation, matrixType: MatrixType,
  vAnnotator: ExprAnnotator, gAnnotator: ExprAnnotator, newRVRowType: TStruct) extends
  SplitMultiPartitionContext(keepStar, nSamples, globalAnnotation, matrixType, newRVRowType) {

  val (t1, locusInserter) = vAnnotator.newT.insert(matrixType.rowType.fieldByName("locus").typ, "locus")
  assert(t1 == vAnnotator.newT)
  val (t2, allelesInserter) = vAnnotator.newT.insert(matrixType.rowType.fieldByName("alleles").typ, "alleles")
  assert(t2 == vAnnotator.newT)

  def constructSplitRow(splitVariants: Iterator[(Variant, Int)], rv: RegionValue, wasSplit: Boolean): Iterator[RegionValue] = {
    val row = fullRow.deleteField(matrixType.entriesIdx)
    val gs = fullRow.getAs[IndexedSeq[Any]](matrixType.entriesIdx)
    splitVariants.map { case (svj, i) =>
      splitRegion.clear()
      rvb.set(splitRegion)
      rvb.start(newRVRowType)
      rvb.startStruct()

      val newLocus = svj.locus
      val newAlleles = svj.alleles

      vAnnotator.ec.setAll(globalAnnotation, newLocus, newAlleles, row, i, wasSplit)
      val newRow = allelesInserter(
        locusInserter(
          vAnnotator.insert(row),
          svj.locus),
        Array(svj.ref, svj.alt).toFastIndexedSeq
      ).asInstanceOf[Row]
      var fdIdx = 0
      while (fdIdx < newRow.length) {
        rvb.addAnnotation(newRVRowType.types(fdIdx), newRow(fdIdx))
        fdIdx += 1
      }

      rvb.startArray(nSamples) // gs
      gAnnotator.ec.setAll(globalAnnotation, newLocus, newAlleles, row, i, wasSplit)
      var k = 0
      while (k < nSamples) {
        val g = gs(k)
        gAnnotator.ec.set(6, g)
        rvb.addAnnotation(gAnnotator.newT, gAnnotator.insert(g))
        k += 1
      }
      rvb.endArray() // gs
      rvb.endStruct()
      splitrv.set(splitRegion, rvb.end())
      splitrv
    }
  }
}


abstract class SplitMultiPartitionContext(
  keepStar: Boolean,
  nSamples: Int, globalAnnotation: Annotation, matrixType: MatrixType, newRVRowType: TStruct) extends Serializable {

  var fullRow = new UnsafeRow(matrixType.rvRowType)
  var prevLocus: Locus = null
  val rvv = new RegionValueVariant(matrixType.rvRowType)
  val splitRegion = Region()
  val rvb = new RegionValueBuilder()
  val splitrv = RegionValue()
  val variantOrdering = matrixType.rowType
    .fieldByName("locus")
    .typ
    .asInstanceOf[TLocus]
    .rg
    .variantOrdering

  def constructSplitRow(splitVariants: Iterator[(Variant, Int)], rv: RegionValue, wasSplit: Boolean): Iterator[RegionValue]

  def splitRow(rv: RegionValue, sortAlleles: Boolean, removeLeftAligned: Boolean, removeMoving: Boolean, verifyLeftAligned: Boolean): Iterator[RegionValue] = {
    require(!(removeMoving && verifyLeftAligned))
    fullRow.set(rv)
    rvv.setRegion(rv)

    var isLeftAligned = true
    if (prevLocus != null && prevLocus == rvv.locus)
      isLeftAligned = false
    val ref = rvv.alleles()(0)
    val alts = rvv.alleles().tail
    var splitVariants = alts.iterator.zipWithIndex
      .filter(keepStar || _._1 != "*")
      .map { case (aa, aai) =>
        val splitv = Variant(rvv.contig(), rvv.position(), ref, aa)
        val minsplitv = splitv.minRep

        if (splitv.locus != minsplitv.locus)
          isLeftAligned = false

        (minsplitv, aai + 1)
      }.toArray

    if (splitVariants.isEmpty)
      return Iterator()

    val wasSplit = alts.length > 1

    if (isLeftAligned) {
      if (removeLeftAligned)
        return Iterator()
    } else {
      if (removeMoving)
        return Iterator()
      else if (verifyLeftAligned)
        fatal(s"found non-left aligned variant: ${ rvv.locus() }:$ref:${ alts.mkString(",") } ")
    }

    if (sortAlleles)
      splitVariants = splitVariants.sortBy { case (svj, i) => svj }(variantOrdering)

    val nAlleles = 1 + alts.length
    val nGenotypes = Variant.nGenotypes(nAlleles)
    constructSplitRow(splitVariants.iterator, rv, wasSplit)
  }
}

object SplitMulti {
  def apply(vsm: MatrixTable): MatrixTable = {
    if (!vsm.entryType.isOfType(Genotype.htsGenotypeType))
      fatal(s"split_multi: genotype_schema must be the HTS genotype schema, found: ${ vsm.entryType }")
    apply(vsm, "va.aIndex = aIndex, va.wasSplit = wasSplit",
      s"""g.`GT` = downcode(g.GT, aIndex),
         g.`AD` = orMissing(isDefined(g.AD), [ (g.AD.sum() - g.AD[aIndex]), g.AD[aIndex] ]),
         g.`DP` = g.DP,
         g.`PL` = orMissing(isDefined(g.PL), range((0), (3), (1))
         	.map(__uid_1 => range((0), triangle(va.alleles.size()), (1))
         	.filter(__uid_2 => (downcode(UnphasedDiploidGtIndexCall(__uid_2), aIndex) == UnphasedDiploidGtIndexCall(__uid_1))).map(__uid_3 => g.PL[__uid_3]).min())),
         g.`GQ` = gqFromPL(orMissing(isDefined(g.PL), range((0), (3), (1)).map(__uid_1 => range((0), triangle(va.alleles.size()), (1)).filter(__uid_2 => (downcode(UnphasedDiploidGtIndexCall(__uid_2), aIndex) == UnphasedDiploidGtIndexCall(__uid_1))).map(__uid_3 => g.PL[__uid_3]).min())))""")
  }

  def apply(vsm: MatrixTable, variantExpr: String, genotypeExpr: String, keepStar: Boolean = false, leftAligned: Boolean = false): MatrixTable = {
    val splitmulti = new SplitMulti(vsm, variantExpr, genotypeExpr, keepStar, leftAligned)
    splitmulti.split()
  }

  def unionMovedVariants(ordered: OrderedRVD,
    moved: RDD[RegionValue]): OrderedRVD = {
    val movedRVD = OrderedRVD.adjustBoundsAndShuffle(ordered.typ,
      ordered.partitioner, moved)

    ordered.copy(orderedPartitioner = movedRVD.partitioner).partitionSortedUnion(movedRVD)
  }
}

class SplitMulti(vsm: MatrixTable, variantExpr: String, genotypeExpr: String, keepStar: Boolean, leftAligned: Boolean) {
  warn(s"splitting")
  val vEC = EvalContext(Map(
    "global" -> (0, vsm.globalType),
    "newLocus" -> (1, vsm.rowKeyStruct.types(0)),
    "newAlleles" -> (2, vsm.rowKeyStruct.types(1)),
    "va" -> (3, vsm.rowType),
    "aIndex" -> (4, TInt32()),
    "wasSplit" -> (5, TBoolean())))
  val vAnnotator = new ExprAnnotator(vEC, vsm.rowType, variantExpr, Some(Annotation.VARIANT_HEAD))

  val gEC = EvalContext(Map(
    "global" -> (0, vsm.globalType),
    "newLocus" -> (1, vsm.rowKeyStruct.types(0)),
    "newAlleles" -> (2, vsm.rowKeyStruct.types(1)),
    "va" -> (3, vsm.rowType),
    "aIndex" -> (4, TInt32()),
    "wasSplit" -> (5, TBoolean()),
    "g" -> (6, vsm.entryType)))
  val gAnnotator = new ExprAnnotator(gEC, vsm.entryType, genotypeExpr, Some(Annotation.GENOTYPE_HEAD))

  val vASTs = Parser.parseAnnotationExprsToAST(variantExpr, vEC, Some("va"))
  val gASTs = Parser.parseAnnotationExprsToAST(genotypeExpr, gEC, Some("g"))
  warn(s"getting IRs")

  val vIRs = vASTs.flatMap { case (name, ast) => for (ir <- ast.toIR()) yield {
    (name, ir)
  }
  }
  val gIRs = gASTs.flatMap { case (name, ast) => for (ir <- ast.toIR()) yield {
    (name, ir)
  }
  }
//
//    info(s"vIRs: \n ${ vIRs.map{ case (n, ir) => n ++ ": " ++ ir.toString }.mkString("\n") }")
//    info(s"gIRs: \n ${ gIRs.map{ case (n, ir) => n ++ ": " ++ ir.toString }.mkString("\n") }")

  val (newMatrixType, useAST, rowF): (MatrixType, Boolean, () => AsmFunction13[Region, Long, Boolean, Long, Boolean, Long, Boolean, Long, Boolean, Int, Boolean, Boolean, Boolean, Long]) =
    if (vASTs.length == vIRs.length && gASTs.length == gIRs.length) {
//    if (false) {
      val ir = new SplitMultiRowIR(vIRs, gIRs, vsm.matrixType)
      warn("Using IR for SplitMulti")
      (ir.newMatrixType, false, ir.splitRow)
    } else {
      val t = vsm.matrixType.copyParts(rowType = vAnnotator.newT, entryType = gAnnotator.newT)
      warn("Falling back to AST for SplitMulti")
      (t, true, null)
    }

  def split(sortAlleles: Boolean, removeLeftAligned: Boolean, removeMoving: Boolean, verifyLeftAligned: Boolean): RDD[RegionValue] = {
    val localKeepStar = keepStar
    val localGlobalAnnotation = vsm.globals
    val localNSamples = vsm.numCols
    val localRowType = vsm.rvRowType
    val localMatrixType = vsm.matrixType
    val localVAnnotator = vAnnotator
    val localGAnnotator = gAnnotator
    val localSplitRow = rowF

    val newRowType = newMatrixType.rvRowType

    val locusIndex = localRowType.fieldIdx("locus")

    if (useAST) {
      vsm.rvd.mapPartitions { it =>
        val context = new SplitMultiPartitionContextAST(localKeepStar, localNSamples, localGlobalAnnotation,
          localMatrixType, localVAnnotator, localGAnnotator, newRowType)
        it.flatMap { rv =>
          val splitit = context.splitRow(rv, sortAlleles, removeLeftAligned, removeMoving, verifyLeftAligned)
          context.prevLocus = context.fullRow.getAs[Locus](locusIndex)
          splitit
        }
      }
    } else {
      vsm.rvd.mapPartitions { it =>
        val context = new SplitMultiPartitionContextIR(localKeepStar, localNSamples, localGlobalAnnotation,
          localMatrixType, localSplitRow, newRowType)
        it.flatMap { rv =>
          val splitit = context.splitRow(rv, sortAlleles, removeLeftAligned, removeMoving, verifyLeftAligned)
          context.prevLocus = context.fullRow.getAs[Locus](locusIndex)
          splitit
        }
      }
    }
  }

  def split(): MatrixTable = {
    val newRDD2: OrderedRVD =
      if (leftAligned)
        OrderedRVD(
          newMatrixType.orvdType,
          vsm.rvd.partitioner,
          split(sortAlleles = true, removeLeftAligned = false, removeMoving = false, verifyLeftAligned = true))
      else
        SplitMulti.unionMovedVariants(OrderedRVD(
          newMatrixType.orvdType,
          vsm.rvd.partitioner,
          split(sortAlleles = true, removeLeftAligned = false, removeMoving = true, verifyLeftAligned = false)),
          split(sortAlleles = false, removeLeftAligned = true, removeMoving = false, verifyLeftAligned = false))

    vsm.copyMT(rvd = newRDD2, matrixType = newMatrixType)
  }
}
