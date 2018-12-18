package is.hail.cxx

import is.hail.annotations._
import is.hail.expr.ir
import is.hail.expr.types.MatrixType
import is.hail.expr.types.physical._
import is.hail.expr.types.virtual.TVoid
import is.hail.nativecode._
import is.hail.rvd.{RVD, RVDContext}
import is.hail.sparkextras.ContextRDD

object Execute {

  private def emit(node: ir.TableIR): TableEmitTriplet = TableEmit(new TranslationUnitBuilder(), node)

  def apply(node: ir.IR): Any = {
    node match {
      case ir.TableWrite(child, path, overwrite, stageLocally, codecSpecJSONStr) =>
        emit(child).write(child.typ, path, overwrite, stageLocally, codecSpecJSONStr)
      case n if n.typ == TVoid =>
        throw new CXXUnsupportedOperation(ir.Pretty(n))
      case _ =>
        val f = Compile(ir.MakeTuple(Array(node)), optimize = true)
        val st = new NativeStatus()
        Region.scoped { region =>
          SafeRow(PTuple(Array(node.pType)), region, f(st, region.get())).get(0)
        }
    }
  }

  def apply(node: ir.TableIR): ir.TableValue = {
    val table = emit(node)
    val ctx = table.t.ctx
    val tub = ctx.tub

    val st = tub.variable("st", "NativeStatus *")
    val makeItF = new FunctionBuilder(tub, "make_iterator", Array(st, ctx.globalsInput, ctx.rddInput), "NativeObjPtr")

    tub.include("hail/PartitionIterators.h")
    val prod = table.t.producer.addArgs(s"&${ ctx.cxxCtx }")
    val itType = s"LinearizedPullStream<${ prod.typ("NestedLinearizerEndpoint") }>"
    makeItF += s"return std::make_shared<ScalaStagingIterator<$itType>>(${ prod.constructorArgs.mkString(", ") });"

    makeItF +=
      s"""try {
         |  ${ ctx.setup }
         |  ${ table.t.setup }
         |  s"return std::make_shared<ScalaStagingIterator<$itType>>(${ prod.constructorArgs.mkString(", ") });"
         |} catch (const FatalError& e) {
         |  NATIVE_ERROR($st, 1006, e.what());
         |  return -1;
         |}
       """.stripMargin
    makeItF.end()

    val modToPtr = { (mod: NativeModule, region: Region, obj: Long) =>
      val st = new NativeStatus()
      val ptrF = mod.findPtrFuncL2(st, "make_iterator")
      assert(st.ok, st.toString())
      val ptr = new NativePtr(ptrF, st, region.get(), obj)
      assert(st.ok, st.toString())
      ptrF.close()
      st.close()
      ptr
    }

    val makeIt = CXXRegionValueIterator(s"ScalaStagingIterator<itType>", tub, modToPtr)

    val crdd =
      ContextRDD.weaken(table.baseRDD, () => RVDContext.default)
        .cmapPartitions { (ctx, long) => makeIt(ctx.region, long.next()) }
    val rvd = RVD(node.typ.canonicalRVDType, table.partitioner, crdd)

    ir.TableValue(node.typ, table.t.ctx.globals, rvd)
  }
}
