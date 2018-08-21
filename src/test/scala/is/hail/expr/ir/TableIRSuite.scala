package is.hail.expr.ir

import is.hail.SparkSuite
import is.hail.annotations.BroadcastIndexedSeq
import is.hail.expr.ir.TestUtils._
import is.hail.expr.types._
import is.hail.table.Table
import is.hail.utils.{FastIndexedSeq, Interval}
import is.hail.variant.MatrixTable
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class TableIRSuite extends SparkSuite {
  def getKT: Table = {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Row.fromSeq(_)))
    val signature = TStruct(("Sample", TString()), ("field1", TInt32()), ("field2", TInt32()))
    val keyNames = IndexedSeq("Sample")

    val kt = Table(hc, rdd, signature, Some(keyNames))
    kt.typeCheck()
    kt
  }

  def rangeKT: TableIR = Table.range(hc, 20, Some(4)).tir

  def getRows(tir: TableIR): Array[Row] = tir.execute(hc).rdd.collect()

  @Test def testFilter() {
    val kt = getKT
    val kt2 = new Table(hc, TableFilter(kt.tir,
      GetField(Ref("row", kt.typ.rowType), "field1").ceq(3)))
    assert(kt2.count() == 1)
  }

  @Test def testScanCountBehavesLikeIndex() {
    val t = rangeKT
    val oldRow = Ref("row", t.typ.rowType)

    val newRow = InsertFields(oldRow, Seq("idx2" -> IRScanCount))
    val newTable = TableMapRows(t, newRow, None, None)
    val rows = Interpret[IndexedSeq[Row]](TableAggregate(newTable, IRAggCollect(Ref("row", newRow.typ))), optimize = false)
    assert(rows.forall { case Row(row_idx, idx) => row_idx == idx})
  }

  @Test def testScanCollectBehavesLikeRange() {
    val t = rangeKT
    val oldRow = Ref("row", t.typ.rowType)

    val newRow = InsertFields(oldRow, Seq("range" -> IRScanCollect(GetField(oldRow, "idx"))))
    val newTable = TableMapRows(t, newRow, None, None)
    val rows = Interpret[IndexedSeq[Row]](TableAggregate(newTable, IRAggCollect(Ref("row", newRow.typ))), optimize = false)
    assert(rows.forall { case Row(row_idx: Int, range: IndexedSeq[Int]) => range sameElements Array.range(0, row_idx)})
  }

  @Test def testTableFilterIntervals() {
    val intervals = BroadcastIndexedSeq(
      FastIndexedSeq(
        Interval(Row(0), Row(2), true, false),
        Interval(Row(7), Row(12), true, false)),
      TArray(TInterval(TStruct("idx" -> TInt32()))),
      hc.sc)

    val filterKeepIdx = getRows(
      TableFilterIntervals(
        TableRange(10, 3),
        intervals,
        keep = true))
      .map { case Row(row_idx) => row_idx }

    assert(filterKeepIdx sameElements Array(0, 1, 7, 8, 9))

    val filterNoKeepIdx = getRows(
      TableFilterIntervals(
        TableRange(10, 3),
        intervals,
        keep = false))
      .map { case Row(row_idx) => row_idx }

    assert(filterNoKeepIdx sameElements Array(2, 3, 4, 5, 6))
  }
}
