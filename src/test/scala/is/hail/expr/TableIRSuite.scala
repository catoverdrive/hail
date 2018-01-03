package is.hail.expr

import is.hail.SparkSuite
import is.hail.expr.types._
import is.hail.table.Table
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class TableIRSuite extends SparkSuite {

  @Test def testFilter() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Row.fromSeq(_)))
    val signature = TStruct(("Sample", TString()), ("field1", TInt32()), ("field2", TInt32()))
    val keyNames = Array("Sample")

    val kt = Table(hc, rdd, signature, keyNames)
    val kt2 = new Table(hc, TableFilter(kt.ir, ir.ApplyBinaryPrimOp(ir.EQ(), ir.Ref("field1"), ir.I32(3))))
    assert(kt2.count() == 1)
  }

  @Test def testFilterGlobals() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Row.fromSeq(_)))
    val signature = TStruct(("Sample", TString()), ("field1", TInt32()), ("field2", TInt32()))
    val keyNames = Array("Sample")

    val kt = Table(hc, rdd, signature, keyNames).annotateGlobalExpr("g = 3")
    val kt2 = new Table(hc, TableFilter(kt.ir, ir.ApplyBinaryPrimOp(ir.EQ(), ir.Ref("field1"), ir.Ref("g"))))
    assert(kt2.count() == 1)
  }

  @Test def testAnnotate() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Row.fromSeq(_)))
    val signature = TStruct(("Sample", TString()), ("field1", TInt32()), ("field2", TInt32()))
    val keyNames = Array("Sample")

    val kt = Table(hc, rdd, signature, keyNames)
    val kt2 = new Table(hc, TableAnnotate(kt.ir, IndexedSeq(List("a"), List("field1", "b")), IndexedSeq(ir.I32(0), ir.Ref("field1"))))
    assert(kt2.select("Sample", "field1 = field1.b", "field2").same(kt))
  }

  @Test def testAnnotate2() {
    val data = Array(Array("Sample1", 9, 5), Array("Sample2", 3, 5), Array("Sample3", 2, 5), Array("Sample4", 1, 5))
    val rdd = sc.parallelize(data.map(Row.fromSeq(_)))
    val signature = TStruct(("Sample", TString()), ("field1", TInt32()), ("field2", TInt32()))
    val keyNames = Array("Sample")

    val kt = Table(hc, rdd, signature, keyNames)
    val kt2 = kt.annotate("a = field1")
    assert(kt2.ir.isInstanceOf[TableAnnotate])
    assert(kt2.select("Sample", "field1 = a", "field2").same(kt))
  }
}
