package is.hail.cxx

import java.io.OutputStream

import is.hail.HailContext
import is.hail.annotations.{BroadcastRow, Region, RegionValueBuilder}
import is.hail.expr.{JSONAnnotationImpex, ir}
import is.hail.expr.types.TableType
import is.hail.expr.types.physical.PStruct
import is.hail.expr.types.virtual.TInt32
import is.hail.io.CodecSpec
import is.hail.nativecode.{NativeModule, NativeStatus, ObjectArray}
import is.hail.rvd.{AbstractRVDSpec, OrderedRVDSpec, RVDPartitioner, RVDType}
import is.hail.table.TableSpec
import is.hail.utils._
import is.hail.variant.{FileFormat, PartitionCountsComponentSpec, RVDComponentSpec, ReferenceGenome}
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods

case class PartitionContext(globals: BroadcastRow, tub: TranslationUnitBuilder) {
  val st: Variable = Variable("st", "NativeStatus*")
  val up: Variable = Variable("up", "UpcallEnv")
  val globalsInput: Variable = Variable("globals", "long")
  val rddInput: Variable = Variable("input_objects", "long")
  val cxxCtx: Variable = Variable("ctx", "PartitionContext", s"{$st, reinterpret_cast<const char *>($globalsInput)}")
  val setup: Code =
    s"""
       |${up.define}
       |${cxxCtx.define}""".stripMargin
}

object PartitionEmitTriplet {
  def read(
    path: String,
    t: PStruct,
    requestedType: PStruct,
    codecSpec: CodecSpec,
    ctx: PartitionContext
  ): PartitionEmitTriplet = {
    val tub = ctx.tub
    tub.include("hail/Decoder.h")
    tub.include("hail/table/TableEmit.h")
    tub.include("hail/table/TableRead.h")
    tub.include("hail/ObjectArray.h")

    val decoder = codecSpec.buildNativeDecoderClass(t, requestedType, tub)
    val is = Variable.make_shared("is", "InputStream", ctx.up.toString, s"reinterpret_cast<ObjectArray *>(${ ctx.rddInput })->at(0)")
    val range = Variable("read_range", s"TablePartitionRange<TableNativeRead<${decoder.name}>>", s"{${decoder.name}($is), &${ctx.cxxCtx}}")

    val setup =
      s"""
         |${ is.define }
         |${ range.define }""".stripMargin

    PartitionEmitTriplet(ctx, setup, range, requestedType)
  }
}

case class PartitionEmitTriplet(ctx: PartitionContext, setup: Code, range: Variable, rowType: PStruct) {
  def write(codecSpec: CodecSpec): Unit = {
    val tub = ctx.tub
    tub.include("hail/Encoder.h")
    val encClass = codecSpec.buildNativeEncoderClass(rowType, tub)

    val os = Variable("os", "long")
    val enc = Variable("encoder", encClass.name, s"std::make_shared<OutputStream>(${ ctx.up }, reinterpret_cast<ObjectArray *>($os)->at(0))")
    val nRows = Variable("n_rows", "long", "0")
    val it = Variable("write_it", s"char *")

    val part = new FunctionBuilder(tub, "process_partition", Array(ctx.st, ctx.globalsInput, ctx.rddInput, os), "long")
    part +=
      s"""
         |${ctx.setup}
         |$setup
         |${ enc.define }
         |${nRows.define}
         |for (auto $it : $range) {
         |  $enc.encode_byte(1);
         |  $enc.encode_row($it);
         |  ++$nRows;
         |}
         |$enc.encode_byte(0);
         |$enc.flush();
         |return $nRows;
       """.stripMargin
    part.end()
  }

}

object TableEmitTriplet {
  def empty(ctx: PartitionContext, typ: RVDType): TableEmitTriplet =
    TableEmitTriplet(
      PartitionEmitTriplet(ctx, "", null, typ.rowType),
      HailContext.get.sc.emptyRDD[Long],
      RVDPartitioner.empty(typ))

  def read(
    path: String,
    t: PStruct,
    codecSpec: CodecSpec,
    partFiles: Array[String],
    requestedType: RVDType,
    partitioner: RVDPartitioner,
    ctx: PartitionContext
  ): TableEmitTriplet = {
    val hc = HailContext.get
    val pet = PartitionEmitTriplet.read(path, t, requestedType.rowType, codecSpec, ctx)
    val partsRDD = hc.readPartitions(path, partFiles, (_, is, m) => Iterator.single(new ObjectArray(is).get()))
    val part = Option(partitioner).getOrElse(RVDPartitioner.unkeyed(partsRDD.getNumPartitions))

    TableEmitTriplet(pet, partsRDD, part)
  }
}

case class TableEmitTriplet(t: PartitionEmitTriplet, baseRDD: RDD[Long], partitioner: RVDPartitioner) {
  def rvdSpec(typ: RVDType, codecSpec: CodecSpec, partFiles: Array[String]) = OrderedRVDSpec(
    typ,
    codecSpec,
    partFiles,
    JSONAnnotationImpex.exportAnnotation(
      partitioner.rangeBounds.toFastSeq,
      partitioner.rangeBoundsType))

  def write(typ: TableType, path: String, overwrite: Boolean, stageLocally: Boolean, codecSpecJSONStr: String) {
    val hc = HailContext.get
    val ctx = t.ctx

    val codecSpec =
      if (codecSpecJSONStr != null) {
        implicit val formats = AbstractRVDSpec.formats
        val codecSpecJSON = JsonMethods.parse(codecSpecJSONStr)
        codecSpecJSON.extract[CodecSpec]
      } else
        CodecSpec.default

    if (overwrite)
      hc.hadoopConf.delete(path, recursive = true)
    else if (hc.hadoopConf.exists(path))
      fatal(s"file already exists: $path")

    hc.hadoopConf.mkDir(path)

    val globalsPath = path + "/globals"
    hc.hadoopConf.mkDir(globalsPath)
    AbstractRVDSpec.writeLocal(hc, globalsPath, typ.globalType.physicalType, codecSpec, Array(ctx.globals.value))

    t.write(codecSpec)

    val mod = ctx.tub.end().build("-O2 -llz4")
    val modKey = mod.getKey
    val modBinary = mod.getBinary
    val gtyp = typ.globalType
    val globs = ctx.globals.broadcast

    val writeF = { (it: Iterator[Long], os: OutputStream) =>
      val st = new NativeStatus()
      val mod = new NativeModule(modKey, modBinary)
      val f = mod.findLongFuncL3(st, "process_partition")
      assert(st.ok, st.toString())
      val osArray = new ObjectArray(os)
      Region.scoped { region =>
        val rvb = new RegionValueBuilder(region)
        rvb.start(gtyp.physicalType)
        rvb.addAnnotation(gtyp, globs.value)
        val nRows = f(st, rvb.end(), it.next(), osArray.get())
        assert(st.ok, st.toString())
        f.close()
        osArray.close()
        mod.close()
        st.close()
        os.flush()
        os.close()
        nRows
      }
    }

    val (partFiles, partitionCounts) = baseRDD.writePartitions(path + "/rows", stageLocally, writeF)
    rvdSpec(typ.canonicalRVDType, codecSpec, partFiles).write(HailContext.get.sc.hadoopConfiguration, path + "/rows")

    val referencesPath = path + "/references"
    hc.hadoopConf.mkDir(referencesPath)
    ReferenceGenome.exportReferences(hc, referencesPath, typ.rowType)
    ReferenceGenome.exportReferences(hc, referencesPath, typ.globalType)

    val spec = TableSpec(
      FileFormat.version.rep,
      hc.version,
      "references",
      typ,
      Map("globals" -> RVDComponentSpec("globals"),
        "rows" -> RVDComponentSpec("rows"),
        "partition_counts" -> PartitionCountsComponentSpec(partitionCounts)))
    spec.write(hc, path)

    hc.hadoopConf.writeTextFile(path + "/_SUCCESS")(out => ())
  }

}

object TableEmit {
  def apply(tub: TranslationUnitBuilder, x: ir.TableIR): TableEmitTriplet = {
    val emitter = new TableEmitter(tub)
    emitter.emit(x)
  }
}

class TableEmitter(tub: TranslationUnitBuilder) {
  outer =>
  type E = ir.Env[TableEmitTriplet]

  def emit(x: ir.TableIR): TableEmitTriplet = emit(x, ir.Env.empty[TableEmitTriplet])

  def emit(x: ir.TableIR, env: E): TableEmitTriplet = {

    def emit(x: ir.TableIR, env: E = env): TableEmitTriplet = this.emit(x, env)

    val typ = x.typ
    x match {
      case ir.TableRead(path, spec, _, dropRows) =>
        val hc = HailContext.get
        val globals = spec.globalsComponent.readLocal(hc, path, typ.globalType.physicalType)(0)
        val ctx = PartitionContext(BroadcastRow(globals, typ.globalType, hc.sc), tub)
        if (dropRows)
          TableEmitTriplet.empty(ctx, typ.canonicalRVDType)
        else {
          val t = spec.rowsComponent.cxxEmitRead(hc, path, typ.rowType, ctx)
          //          if (rvd.typ.key startsWith typ.key)
          //            rvd
          //          else {
          //            log.info("Sorting a table after read. Rewrite the table to prevent this in the future.")
          //            rvd.changeKey(typ.key)
          //          }
          t
        }

      case ir.TableMapRows(child, newRow) =>
        val prev = emit(child)
        val ctx = prev.t.ctx
        tub.include("hail/table/TableMapRows.h")
        val mapper = tub.buildClass(tub.genSym("Mapper"))

        val mapF = mapper.buildMethod("operator()",
          Array("NativeStatus*" -> "st",
            "Region*" -> "region",
            "const char *" -> "globals",
            "const char *" -> "row"), "const char *")
        val substEnv = ir.Env.empty[ir.IR]
          .bind("globals", ir.In(1, child.typ.globalType))
          .bind("row", ir.In(2, child.typ.rowType))
        val et = Emit(mapF, 1, ir.Subst(newRow, substEnv))
        mapF +=
          s"""
             |${ et.setup }
             |if (${ et.m }) {
             |  ${ mapF.nativeError(1011, "\"mapped row can't be missing!\"") }
             |} else {
             |  return ${ et.v };
             |}
           """.stripMargin
        mapF.end()
        mapper.end()

        val range = Variable("map_range",
          s"TablePartitionRange<TableMapRows<${ prev.t.range.typ }, ${ mapper.name }>>",
          s"{&${ ctx.cxxCtx }, ${ prev.t.range }}")

        val setup =
          s"""
             |${ prev.t.setup }
             |${ range.define }""".stripMargin

        val newPet = PartitionEmitTriplet(ctx, setup, range, newRow.typ.physicalType.asInstanceOf[PStruct])
        prev.copy(t = newPet)

      case ir.TableFilter(child, cond) =>
        val prev = emit(child)
        val prevPartition: PartitionEmitTriplet = prev.t
        val ctx = prevPartition.ctx
        tub.include("hail/table/TableFilterRows.h")
        val filter = tub.buildClass(tub.genSym("Filter"))

        val filterF = filter.buildMethod("operator()",
          Array("NativeStatus*" -> "st",
            "Region*" -> "region",
            "const char *" -> "globals",
            "const char *" -> "row"),
          "bool")
        val substEnv = ir.Env.empty[ir.IR]
          .bind("globals", ir.In(1, child.typ.globalType))
          .bind("row", ir.In(2, child.typ.rowType))
        val et = Emit(filterF, 1, ir.Subst(cond, substEnv))
        filterF +=
          s"""
             |${ et.setup }
             |return !(${ et.m }) && (${ et.v });
           """.stripMargin
        filterF.end()
        filter.end()

        val range = Variable("filter_range",
          s"TablePartitionRange<TableFilterRows<${ prevPartition.range.typ }, ${ filter.name }>>",
          s"{&${ ctx.cxxCtx }, ${ prevPartition.range }}")

        val setup =
          s"""
             |${ prevPartition.setup }
             |${ range.define }
       """.stripMargin

        prev.copy(t = prevPartition.copy(setup = setup, range = range))
      case x@ir.TableExplode(child, fname) =>
        val prev = emit(child)
        val ctx = prev.t.ctx
        tub.include("hail/table/TableExplodeRows.h")
        val exploder = tub.buildClass(genSym("Exploder"))

        val lenF = exploder.buildMethod("len",
          Array("NativeStatus*" -> "st",
            "Region*" -> "region",
            "const char *" -> "row"), "int")
        val lenEnv = ir.Env.empty[ir.IR]
          .bind("row", ir.In(1, child.typ.rowType))
        val lent = Emit(lenF, 1, ir.Subst(x.lengthIR, lenEnv))
        lenF +=
          s"""
             |${ lent.setup }
             |return (${ lent.m }) ? 0 : (${ lent.v });
           """.stripMargin
        lenF.end()

        val explodeF = exploder.buildMethod("operator()",
          Array("NativeStatus*" -> "st",
            "Region*" -> "region",
            "const char *" -> "row",
            "int" -> "i"), "const char *")
        val substEnv = ir.Env.empty[ir.IR]
          .bind("row", ir.In(1, child.typ.rowType))
          .bind("i", ir.In(2, TInt32()))
        val et = Emit(explodeF, 1, ir.Subst(x.insertIR, substEnv))
        explodeF +=
          s"""
             |${ et.setup }
             |if (${ et.m }) {
             |  ${ explodeF.nativeError(1011, "\"exploded row can't be missing!\"") }
             |} else {
             |  return ${ et.v };
             |}
           """.stripMargin
        explodeF.end()
        exploder.end()

        val range = Variable("map_range",
          s"TablePartitionRange<TableExplodeRows<${ prev.t.range.typ }, ${ exploder.name }>>",
          s"{&${ ctx.cxxCtx }, ${ prev.t.range }}")

        val setup =
          s"""
             |${ prev.t.setup }
             |${ range.define }""".stripMargin

        val newPet = PartitionEmitTriplet(ctx, setup, range, x.typ.rowType.physicalType.asInstanceOf[PStruct])
        prev.copy(t = newPet)

      case _ =>
        throw new CXXUnsupportedOperation(ir.Pretty(x))
    }
  }
}
