package is.hail.methods

import is.hail.annotations._
import is.hail.asm4s.{AsmFunctionN, FunctionBuilder}
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

class SplitMultiRowIR(vIR: Array[(String, IR)], entryIR: Array[(String, IR)], oldMatrixType: MatrixType) extends Serializable {
  val oldRowIR = Ref("va")
  val newEntries = ArrayMap(GetField(In(3, oldMatrixType.rvRowType), MatrixType.entriesIdentifier) ,"g", MakeStruct(entryIR))
  val changedFields: Seq[(String, IR)] =
    (vIR
      :+ ("locus", Ref("newLocus"))
      :+ ("alleles", Ref("newAlleles"))
      :+ (MatrixType.entriesIdentifier, newEntries))

  val newRowIR: IR = InsertFields(oldRowIR, changedFields)
  private val (t: Type, f: () => AsmFunctionN[Long]) = Compile[Long](Array(
    ("global", RegionValueRep[Long](oldMatrixType.globalType)),
    ("newLocus", RegionValueRep[Long](oldMatrixType.rowKeyStruct.types(0))),
    ("newAlleles", RegionValueRep[Long](oldMatrixType.rowKeyStruct.types(1))),
    ("va", RegionValueRep[Long](oldMatrixType.rvRowType)),
    ("aIndex", RegionValueRep[Int](TInt32())),
    ("wasSplit", RegionValueRep[Boolean](TBoolean()))),
    newRowIR)

  val newMatrixType: MatrixType = oldMatrixType.copy(rvRowType = coerce[TStruct](t))
  val splitRow = new SplitRow(f)

}

class SplitRow(f: () => AsmFunctionN[Long]) extends Serializable {
  val f2 = f()
  def apply(region: Region, globalOff: Long, locusOff: Long, allelesOff: Long, rowOff: Long, aIndex: Int, wasSplit: Boolean): Long =
    f2(region, globalOff, true, locusOff, true, allelesOff, true, rowOff, true, aIndex, true, wasSplit, true)
}

class SplitMultiPartitionContextIR(
  keepStar: Boolean,
  nSamples: Int, globalAnnotation: Annotation, matrixType: MatrixType,
  rowF: SplitRow, newRVRowType: TStruct) extends
  SplitMultiPartitionContext(keepStar, nSamples, globalAnnotation, matrixType, newRVRowType) {

  private var globalsCopied = false
  private var globals: Long = 0
  private var globalsEnd: Long = 0

  private def copyGlobals() {
    splitRegion.clear()
    rvb.start(matrixType.globalType)
    rvb.addAnnotation(matrixType.globalType, globalAnnotation)
    globals = rvb.end()
    globalsEnd = splitRegion.size
    globalsCopied = true
  }

  private val allelesType = matrixType.rowType.fieldByName("alleles").typ
  private val locusType = matrixType.rowType.fieldByName("locus").typ

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

      val off = rowF(splitRegion, globals, locusOff, allelesOff, oldRow, aIndex, wasSplit)
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
  nSamples: Int, globalAnnotation: Annotation, matrixType: MatrixType, newRVRowType: TStruct) {

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
      s"""g =
    let
      newc = downcode(g.GT, aIndex) and
      newad = if (isDefined(g.AD))
          let sum = g.AD.sum() and adi = g.AD[aIndex] in [sum - adi, adi]
        else
          NA: Array[Int] and
      newpl = if (isDefined(g.PL))
          range(3).map(i => range(g.PL.length).filter(j => downcode(UnphasedDiploidGtIndexCall(j), aIndex) == UnphasedDiploidGtIndexCall(i)).map(j => g.PL[j]).min())
        else
          NA: Array[Int] and
      newgq = gqFromPL(newpl)
    in { GT: newc, AD: newad, DP: g.DP, GQ: newgq, PL: newpl }""")
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

  val vASTs = Parser.parseAnnotationExprsToAST(variantExpr, vEC)
  val gASTs = Parser.parseAnnotationExprsToAST(genotypeExpr, gEC)

  val vIRs = vASTs.flatMap { case (name, ast) => for (ir <- ast.toIR()) yield { (name, ir) } }
  val gIRs = gASTs.flatMap { case (name, ast) => for (ir <- ast.toIR()) yield { (name, ir) } }

  var newMatrixType: MatrixType = null
  var getPartitionContext: (Int, Annotation, MatrixType) => SplitMultiPartitionContext = null

  if (vASTs.length == vIRs.length && gASTs.length == gIRs.length) {
    val ir = new SplitMultiRowIR(vIRs, gIRs, vsm.matrixType)
    val rowF = ir.splitRow
    newMatrixType = ir.newMatrixType
    getPartitionContext = (nSamples: Int, globals: Annotation, matrixType: MatrixType) =>
      new SplitMultiPartitionContextIR(keepStar, nSamples, globals, matrixType, rowF, newMatrixType.rvRowType)
  } else {
    info("Falling back to AST for SplitMulti")
    newMatrixType = vsm.matrixType.copyParts(rowType = vAnnotator.newT, entryType = gAnnotator.newT)
    getPartitionContext = (nSamples: Int, globals: Annotation, matrixType: MatrixType) =>
      new SplitMultiPartitionContextAST(keepStar, nSamples, globals, matrixType, vAnnotator, gAnnotator, newMatrixType.rvRowType)
  }




  def split(sortAlleles: Boolean, removeLeftAligned: Boolean, removeMoving: Boolean, verifyLeftAligned: Boolean): RDD[RegionValue] = {
    val localKeepStar = keepStar
    val localGlobalAnnotation = vsm.globals
    val localNSamples = vsm.numCols
    val localRowType = vsm.rvRowType
    val localMatrixType = vsm.matrixType
    val localVAnnotator = vAnnotator
    val localGAnnotator = gAnnotator

    val newRowType = newMatrixType.rvRowType

    val locusIndex = localRowType.fieldIdx("locus")

    vsm.rvd.mapPartitions { it =>
      val context = getPartitionContext(localNSamples, localGlobalAnnotation, localMatrixType)

      it.flatMap { rv =>
        val splitit = context.splitRow(rv, sortAlleles, removeLeftAligned, removeMoving, verifyLeftAligned)
        context.prevLocus = context.fullRow.getAs[Locus](locusIndex)
        splitit
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
