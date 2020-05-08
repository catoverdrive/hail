package is.hail.io.index

import java.io.OutputStream

import is.hail.annotations.{Annotation, Region, RegionValueBuilder, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{CodeParam, EmitClassBuilder, EmitCodeBuilder, EmitFunctionBuilder, EmitMethodBuilder, ExecuteContext, IEmitCode, ParamType, coerce}
import is.hail.expr.types
import is.hail.expr.types._
import is.hail.expr.types.encoded.EType
import is.hail.expr.types.physical.{PBaseStruct, PBaseStructValue, PCanonicalArray, PCanonicalBaseStructSettable, PCanonicalStruct, PCode, PInt64, PType}
import is.hail.expr.types.virtual.Type
import is.hail.io.fs.FS
import is.hail.io._
import is.hail.rvd.AbstractRVDSpec
import is.hail.utils._
import is.hail.utils.richUtils.ByteTrackingOutputStream
import org.json4s.Formats
import org.json4s.jackson.Serialization

trait AbstractIndexMetadata {
  def fileVersion: Int

  def branchingFactor: Int

  def height: Int

  def keyType: Type

  def annotationType: Type

  def nKeys: Long

  def indexPath: String

  def rootOffset: Long

  def attributes: Map[String, Any]
}

case class IndexMetadataUntypedJSON(
  fileVersion: Int,
  branchingFactor: Int,
  height: Int,
  nKeys: Long,
  indexPath: String,
  rootOffset: Long,
  attributes: Map[String, Any]
) {
  def toMetadata(keyType: Type, annotationType: Type): IndexMetadata = IndexMetadata(
    fileVersion, branchingFactor,
    height, keyType, annotationType,
    nKeys, indexPath, rootOffset, attributes)
}

case class IndexMetadata(
  fileVersion: Int,
  branchingFactor: Int,
  height: Int,
  keyType: Type,
  annotationType: Type,
  nKeys: Long,
  indexPath: String,
  rootOffset: Long,
  attributes: Map[String, Any]
) extends AbstractIndexMetadata

case class IndexNodeInfo(
  indexFileOffset: Long,
  firstIndex: Long,
  firstKey: Annotation,
  firstRecordOffset: Long,
  firstAnnotation: Annotation
)

object IndexWriter {
  val version: SemanticVersion = SemanticVersion(1, 1, 0)

  val spec: BufferSpec = BufferSpec.default
  def builder(
    ctx: ExecuteContext,
    keyType: PType,
    annotationType: PType,
    branchingFactor: Int = 4096,
    attributes: Map[String, Any] = Map.empty[String, Any]
  ): String => IndexWriter = {
    val f = StagedIndexPartitionWriter.build(ctx, keyType, annotationType, branchingFactor, attributes);
    { path: String =>
      new IndexWriter(keyType, annotationType, f(path))
    }
  }
}

class IndexWriter(keyType: PType, valueType: PType, comp: CompiledIndexWriter) extends AutoCloseable {
  private val region = Region()
  private val rvb = new RegionValueBuilder(region)
  def +=(x: Annotation, offset: Long, annotation: Annotation): Unit = {
    rvb.start(keyType)
    rvb.addAnnotation(keyType.virtualType, x)
    val koff = rvb.end()
    rvb.start(valueType)
    rvb.addAnnotation(valueType.virtualType, annotation)
    val voff = rvb.end()
    comp.apply(koff, offset, voff)
  }

  def close(): Unit = {
    region.close()
    comp.close()
  }
}

class IndexWriterArrayBuilder(name: String, maxSize: Int, sb: SettableBuilder, region: Value[Region], arrayType: PCanonicalArray) {
  private val aoff = sb.newSettable[Long](s"${name}_aoff")
  private val len = sb.newSettable[Int](s"${name}_len")

  val eltType: PCanonicalStruct = types.coerce[PCanonicalStruct](arrayType.elementType)
  private val elt = new PCanonicalBaseStructSettable(eltType, sb.newSettable[Long](s"${name}_elt_off"))

  def length: Code[Int] = len

  def loadFrom(cb: EmitCodeBuilder, a: Code[Long], l: Code[Int]): Unit = {
    cb += (aoff := a)
    cb += (len := l)
  }

  def create(cb: EmitCodeBuilder, dest: Code[Long]): Unit = {
    cb += (aoff := arrayType.allocate(region, maxSize))
    cb += arrayType.stagedInitialize(aoff, maxSize)
    cb += PCode(arrayType, aoff).store(cb.emb, region, dest)
    cb += (len := 0)
  }

  def storeLength(cb: EmitCodeBuilder): Unit = cb += arrayType.storeLength(aoff, length)

  def setFieldValue(cb: EmitCodeBuilder, name: String, field: PCode): Unit = {
    cb += eltType.setFieldPresent(elt.a, name)
    cb += StagedRegionValueBuilder.deepCopy(cb.emb.ecb, region, eltType.fieldType(name), field.code, eltType.fieldOffset(elt.a, name))
  }

  def setField(cb: EmitCodeBuilder, name: String, v: => IEmitCode): Unit =
    v.consume(cb,
      cb += eltType.setFieldMissing(elt.a, name),
      setFieldValue(cb, name, _))

  def addChild(cb: EmitCodeBuilder): Unit = {
    loadChild(cb, len)
    cb += (len := len + 1)
  }
  def loadChild(cb: EmitCodeBuilder, idx: Code[Int]): Unit = cb += elt.store(PCode(eltType, arrayType.elementOffset(aoff, idx)))
  def getLoadedChild: PBaseStructValue = elt

  def getChild(idx: Value[Int]): PCode = PCode(eltType, arrayType.elementOffset(aoff, idx))
}

class StagedIndexWriterUtils(ib: Settable[IndexWriterUtils]) {
  def create(path: Code[String], fs: Code[FS], meta: Code[StagedIndexMetadata]): Code[Unit] =
    ib := Code.newInstance[IndexWriterUtils, String, FS, StagedIndexMetadata](path, fs, meta)
  def size: Code[Int] = ib.invoke[Int]("size")
  def add(r: Code[Region], aoff: Code[Long], len: Code[Int]): Code[Unit] =
    ib.invoke[Region, Long, Int, Unit]("add", r, aoff, len)

  def update(idx: Code[Int], r: Code[Region], aoff: Code[Long], len: Code[Int]): Code[Unit] =
    ib.invoke[Int, Region, Long, Int, Unit]("update", idx, r, aoff, len)

  def getRegion(idx: Code[Int]): Code[Region] = ib.invoke[Int, Region]("getRegion", idx)
  def getArrayOffset(idx: Code[Int]): Code[Long] = ib.invoke[Int, Long]("getArrayOffset", idx)
  def getLength(idx: Code[Int]): Code[Int] = ib.invoke[Int, Int]("getLength", idx)
  def close(): Code[Unit] = ib.invoke[Unit]("close")

  def bytesWritten: Code[Long] = ib.invoke[Long]("bytesWritten")
  def os: Code[OutputStream] = ib.invoke[OutputStream]("os")

  def writeMetadata(height: Code[Int], rootOffset: Code[Long], nKeys: Code[Long]): Code[Unit] =
    ib.invoke[Int, Long, Long, Unit]("writeMetadata", height, rootOffset, nKeys)
}

class InternalNodeArrayBuilder(initSize: Int) {
}

case class StagedIndexMetadata(
  branchingFactor: Int,
  keyType: Type,
  annotationType: Type,
  attributes: Map[String, Any]
) {
  def serialize(out: OutputStream, height: Int, rootOffset: Long, nKeys: Long) {
    import AbstractRVDSpec.formats
    val metadata = IndexMetadata(IndexWriter.version.rep, branchingFactor, height, keyType, annotationType, nKeys, "index", rootOffset, attributes)
    Serialization.write(metadata, out)
  }
}

class IndexWriterUtils(path: String, fs: FS, meta: StagedIndexMetadata) {
  val indexPath: String = path + "/index"
  val metadataPath: String = path + "/metadata.json.gz"
  val trackedOS: ByteTrackingOutputStream = new ByteTrackingOutputStream(fs.create(indexPath))

  def bytesWritten: Long = trackedOS.bytesWritten
  def os: OutputStream = trackedOS

  def writeMetadata(height: Int, rootOffset: Long, nKeys: Long): Unit = {
    using(fs.create(metadataPath)) { os => meta.serialize(os, height, rootOffset, nKeys) }
  }

  val rBuilder = new ArrayBuilder[Region]()
  val aBuilder = new ArrayBuilder[Long]()
  val lBuilder = new ArrayBuilder[Int]()

  def size: Int = rBuilder.size

  def add(r: Region, aoff: Long, len: Int): Unit = {
    rBuilder += r
    aBuilder += aoff
    lBuilder += len
  }

  def update(idx: Int, r: Region, aoff: Long, len: Int): Unit = {
    if (idx == size) {
      add(r, aoff, len)
    } else {
      rBuilder.update(idx, r)
      aBuilder.update(idx, aoff)
      lBuilder.update(idx, len)
    }
  }

  def getRegion(idx: Int): Region = rBuilder(idx)
  def getArrayOffset(idx: Int): Long = aBuilder(idx)
  def getLength(idx: Int): Int = lBuilder(idx)

  def close(): Unit = {
    rBuilder.result().foreach { r => r.close() }
    trackedOS.close()
  }
}

trait CompiledIndexWriter {
  def init(path: String): Unit
  def apply(x: Long, offset: Long, annotation: Long): Unit
  def close(): Unit
}

object StagedIndexPartitionWriter {
  def build(
    ctx: ExecuteContext,
    keyType: PType,
    annotationType: PType,
    branchingFactor: Int = 4096,
    attributes: Map[String, Any] = Map.empty[String, Any]
  ): String => CompiledIndexWriter = {
    val fb = EmitFunctionBuilder[CompiledIndexWriter](ctx, "indexwriter",
      FastIndexedSeq[ParamType](typeInfo[Long], typeInfo[Long], typeInfo[Long]),
      typeInfo[Unit])
    val cb = fb.ecb
    val siw = new StagedIndexPartitionWriter(branchingFactor, keyType, annotationType, attributes, cb)

    cb.newEmitMethod("init", FastIndexedSeq[ParamType](typeInfo[String]), typeInfo[Unit])
      .voidWithBuilder(cb => siw.init(cb, cb.emb.getCodeParam[String](1)))
    fb.emb.voidWithBuilder { cb =>
      siw.add(cb,
        IEmitCode(cb, false, PCode(keyType, fb.getCodeParam[Long](1))),
        fb.getCodeParam[Long](2),
        IEmitCode(cb, false, PCode(annotationType, fb.getCodeParam[Long](3))))
    }
    cb.newEmitMethod("close", FastIndexedSeq[ParamType](), typeInfo[Unit])
      .voidWithBuilder(siw.close)
    val makeFB = fb.resultWithIndex()

    { path: String =>
      val f = makeFB(0, null)
      f.init(path)
      f
    }
  }
}

class StagedIndexPartitionWriter(branchingFactor: Int, keyType: PType, annotationType: PType, attributes: Map[String, Any], cb: EmitClassBuilder[_]) {
  require(branchingFactor > 1)

  private var elementIdx = cb.genFieldThisRef[Long]()
  private val ob = cb.genFieldThisRef[OutputBuffer]()
  private val utils = new StagedIndexWriterUtils(cb.genFieldThisRef[IndexWriterUtils]())

  private val leafBuilder = new StagedLeafNodeBuilder(branchingFactor, keyType, annotationType, cb.fieldBuilder)
  private val writeInternalNode: EmitMethodBuilder[_] = {
    val m = cb.genEmitMethod[Int, Boolean, Unit]("writeInternalNode")

    val internalBuilder = new StagedInternalNodeBuilder(branchingFactor, keyType, annotationType, m.localBuilder)
    val parentBuilder = new StagedInternalNodeBuilder(branchingFactor, keyType, annotationType, m.localBuilder)

    m.emitWithBuilder { cb =>
      val level = m.getCodeParam[Int](1)
      val isRoot = m.getCodeParam[Boolean](2)
      val idxOff = cb.newLocal[Long]("indexOff")
      cb += (idxOff := utils.bytesWritten)
      internalBuilder.loadFrom(cb, utils, level)
      cb += ob.writeByte(1.toByte)
      internalBuilder.encode(cb, ob)
      cb += ob.flush()

      val next = m.newLocal[Int]("next")
      cb += (next := level + 1)
      cb.ifx(!isRoot, {
        cb.ifx(utils.size.ceq(next),
          parentBuilder.create(cb), {
            cb.ifx(utils.getLength(next).ceq(branchingFactor),
              cb += m.invokeCode[Unit](CodeParam(next), CodeParam(false)))
            parentBuilder.loadFrom(cb, utils, next)
          })
        internalBuilder.loadChild(cb, 0)
        parentBuilder.add(cb, idxOff, internalBuilder.getLoadedChild)
        parentBuilder.store(cb, utils, next)
      })

      internalBuilder.reset(cb)
      internalBuilder.store(cb, utils, level)
      Code._empty
    }
    m
  }

  private val writeLeafNode: EmitMethodBuilder[_] = {
    val m = cb.genEmitMethod[Unit]("writeLeafNode")

    val parentBuilder = new StagedInternalNodeBuilder(branchingFactor, keyType, annotationType, m.localBuilder)
    m.emitWithBuilder { cb =>
      val idxOff = cb.newLocal[Long]("indexOff")
      cb += (idxOff := utils.bytesWritten)
      cb += ob.writeByte(0.toByte)
      leafBuilder.encode(cb, ob)
      cb += ob.flush()

      cb.ifx(utils.getLength(0).ceq(branchingFactor),
        cb += writeInternalNode.invokeCode[Unit](CodeParam(0), CodeParam(false)))
      parentBuilder.loadFrom(cb, utils, 0)

      leafBuilder.loadChild(cb, 0)
      parentBuilder.add(cb, idxOff, leafBuilder.firstIdx.tcode[Long], leafBuilder.getLoadedChild)
      parentBuilder.store(cb, utils, 0)
      leafBuilder.reset(cb, elementIdx)
      Code._empty
    }
    m
  }

  private val flush: EmitMethodBuilder[_] = {
    val m = cb.genEmitMethod[Long]("flush")
    m.emitWithBuilder { cb =>
      val idxOff = cb.newLocal[Long]("indexOff")
      val level = m.newLocal[Int]("level")
      cb.ifx(leafBuilder.ab.length > 0, cb += writeLeafNode.invokeCode[Unit]())
      cb += (level := 0)
      cb.whileLoop(level < utils.size - 1, {
        cb.ifx(utils.getLength(level) > 0,
          cb += writeInternalNode.invokeCode[Unit](CodeParam(level), CodeParam(false)))
        cb += (level := level + 1)
      })
      cb += (idxOff := utils.bytesWritten)
      cb += writeInternalNode.invokeCode[Unit](CodeParam(level), CodeParam(true))
      idxOff.load()
    }
    m
  }

  def add(cb: EmitCodeBuilder, key: => IEmitCode, offset: Code[Long], annotation: => IEmitCode) {
    cb.ifx(leafBuilder.ab.length.ceq(branchingFactor),
      cb += writeLeafNode.invokeCode[Unit]())
    leafBuilder.add(cb, key, offset, annotation)
    cb += (elementIdx := elementIdx + 1L)
  }
  def close(cb: EmitCodeBuilder): Unit = {
    val off = cb.newLocal[Long]("lastOffset")
    cb += (off := flush.invokeCode[Long]())
    leafBuilder.close(cb)
    cb += utils.close()
    cb += utils.writeMetadata(utils.size + 1, off, elementIdx)
  }

  def init(cb: EmitCodeBuilder, path: Value[String]): Unit = {
    val metadata = cb.emb.getObject(StagedIndexMetadata(
      branchingFactor,
      keyType.virtualType,
      annotationType.virtualType,
      attributes))
    val internalBuilder = new StagedInternalNodeBuilder(branchingFactor, keyType, annotationType, cb.localBuilder)
    cb += (elementIdx := 0L)
    cb += utils.create(path, cb.emb.getFS, metadata)
    cb += (ob := IndexWriter.spec.buildCodeOutputBuffer(utils.os))
    leafBuilder.create(cb, 0L)
    internalBuilder.create(cb)
    internalBuilder.store(cb, utils, 0)
  }
}