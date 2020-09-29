package is.hail.backend.service

import java.io.{DataOutputStream, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, PrintWriter, RandomAccessFile, StringWriter}

import is.hail.HailContext
import is.hail.annotations.{Region, UnsafeRow}
import is.hail.asm4s._
import is.hail.backend.{Backend, BackendContext, BroadcastValue}
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.lowering.{DArrayLowering, LowerDistributedSort, LowererUnsupportedOperation, LoweringPipeline, TableStage}
import is.hail.expr.ir.{Compile, ExecuteContext, IR, IRParser, MakeTuple, SortField}
import is.hail.types.physical.{PBaseStruct, PType}
import is.hail.io.fs.{FS, GoogleStorageFS}
import is.hail.linalg.BlockMatrix
import is.hail.services.batch_client.{BatchClient, JavaBatchSpec, JavaJobSpec}
import is.hail.types.BlockMatrixType
import is.hail.types.virtual.Type
import is.hail.utils._
import org.apache.commons.io.IOUtils
import org.apache.log4j.LogManager
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.{JArray, JBool, JInt, JObject, JString}
import org.json4s.jackson.JsonMethods

import scala.collection.mutable
import scala.reflect.ClassTag

object RunWorker {
  def time[T](label: String)(f: => T): T = {
    val start = System.nanoTime()
    val res = f
    val end = System.nanoTime()
    val time = (end - start).toDouble / (1000 * 1000 * 1000).toDouble
    println(s"TIME: $label: $time seconds")
    res
  }

  def main(args: Array[String]): Unit = {
    val root = args(0)
    val idx = args(1)
    val nItersTotal = args(2).toInt
    val nItersPerKey = args(3).toInt
    val keyFiles = args.slice(4, args.length)

    var i = 0
    while (i < nItersTotal) {
      keyFiles.foreach { keyFile =>
        var j = 0
        while(j < nItersPerKey) {
          val start = System.nanoTime()
          Worker.main(Array(root, idx, keyFile))
          val end = System.nanoTime()
          val time = (end - start).toDouble / (1000 * 1000 * 1000).toDouble
          println(s"single iteration: $time seconds")
          j += 1
        }
      }
      i += 1
    }
  }
}

object Worker {
  def main(args: Array[String]): Unit = {
    if (args.length != 2 && args.length != 3)
      throw new IllegalArgumentException(s"expected one argument, not: ${ args.length }")

    val root = args(0)
    val i = args(1).toInt

    val fs = if (args.length == 3) GoogleStorageFS.fromFile(args(2)) else GoogleStorageFS()

    val f = using(new ObjectInputStream(fs.openNoCompression(s"$root/f"))) { is =>
      is.readObject().asInstanceOf[(Array[Byte], Int) => Array[Byte]]
    }

    val context = using(fs.openNoCompression(s"$root/contexts")) { is =>
      is.seek(i * 12)
      val offset = is.readLong()
      val length = is.readInt()
      is.seek(offset)
      val context = new Array[Byte](length)
      is.readFully(context)
      context
    }

    val result = f(context, i)

    using(fs.createNoCompression(s"$root/result.$i")) { os =>
      os.write(result)
    }
  }
}

class ServiceBackendContext(
  val username: String,
  @transient val sessionID: String,
  val billingProject: String,
  val bucket: String
) extends BackendContext

object ServiceBackend {
  lazy val log = LogManager.getLogger("is.hail.backend.service.ServiceBackend")

  def apply(): ServiceBackend = {
    new ServiceBackend()
  }
}

class User(
  val username: String,
  val tmpdir: String,
  val fs: GoogleStorageFS)

final class Response(val status: Int, val value: String)

class ServiceBackend() extends Backend {
  import ServiceBackend.log

  private[this] val users = mutable.Map[String, User]()

  def addUser(username: String, key: String): Unit = {
    assert(!users.contains(username))
    users += username -> new User(username, "/tmp", new GoogleStorageFS(key))
  }

  def removeUser(username: String): Unit = {
    assert(users.contains(username))
    users -= username
  }

  def userContext[T](username: String)(f: (ExecuteContext) => T): T = {
    val user = users(username)
    ExecuteContext.scoped(user.tmpdir, "file:///tmp", this, user.fs)(f)
  }

  def defaultParallelism: Int = 10

  def broadcast[T: ClassTag](_value: T): BroadcastValue[T] = new BroadcastValue[T] with Serializable {
    def value: T = _value
  }

  def parallelizeAndComputeWithIndex(_backendContext: BackendContext, collection: Array[Array[Byte]])(f: (Array[Byte], Int) => Array[Byte]): Array[Array[Byte]] = {
    val workerImage = HailContext.getFlag("worker_image")
    assert(workerImage != null)
    log.info(s"parallelizeAndComputeWithIndex: using worker image $workerImage")
    val backendContext = _backendContext.asInstanceOf[ServiceBackendContext]

    val user = users(backendContext.username)
    val fs = user.fs

    val n = collection.length

    val token = tokenUrlSafe(32)

    log.info(s"parallelizeAndComputeWithIndex: nPartitions $n token $token")

    val root = s"gs://${ backendContext.bucket }/tmp/hail/query/$token"

    log.info(s"parallelizeAndComputeWithIndex: token $token: writing f")

    using(new ObjectOutputStream(fs.create(s"$root/f"))) { os =>
      os.writeObject(f)
    }

    log.info(s"parallelizeAndComputeWithIndex: token $token: writing contexts")

    using(fs.createNoCompression(s"$root/contexts")) { os =>
      var o = 12L * n
      var i = 0
      while (i < n) {
        val len = collection(i).length
        os.writeLong(o)
        os.writeInt(len)
        i += 1
        o += len
      }

      collection.foreach { context =>
        os.write(context)
      }
    }

    val jobs = new Array[JavaJobSpec](n)
    var i = 0
    while (i < n) {
      jobs(i) = new JavaJobSpec(s"parallelizeAndComputeWithIndex_$i", workerImage, Worker.getClass.getCanonicalName, FastIndexedSeq(root, i.toString))
      i += 1
    }

    log.info(s"parallelizeAndComputeWithIndex: token $token: running job")

    val batchClient = BatchClient.fromSessionID(backendContext.sessionID)
    JavaBatchSpec(s"query_cda_${ tokenUrlSafe(5) }", backendContext.billingProject, token, jobs).run(batchClient)

    log.info(s"parallelizeAndComputeWithIndex: token $token: reading results")

    val r = new Array[Array[Byte]](n)
    i = 0  // reusing
    while (i < n) {
      r(i) = using(fs.openNoCompression(s"$root/result.$i")) { is =>
        IOUtils.toByteArray(is)
      }
      i += 1
    }
    r
  }

  def stop(): Unit = ()

  def formatException(e: Exception): String = {
    using(new StringWriter()) { sw =>
      using(new PrintWriter(sw)) { pw =>
        e.printStackTrace(pw)
        sw.toString
      }
    }
  }

  def statusForException(f: => String): Response = {
    try {
      new Response(200, f)
    } catch {
      case e: HailException =>
        new Response(400, formatException(e))
      case e: Exception =>
        new Response(500, formatException(e))
    }
  }

  def valueType(username: String, s: String): Response = {
    statusForException {
      userContext(username) { ctx =>
        val x = IRParser.parse_value_ir(ctx, s)
        x.typ.toString
      }
    }
  }

  def tableType(username: String, s: String): Response = {
    statusForException {
      userContext(username) { ctx =>
        val x = IRParser.parse_table_ir(ctx, s)
        val t = x.typ
        val jv = JObject("global" -> JString(t.globalType.toString),
          "row" -> JString(t.rowType.toString),
          "row_key" -> JArray(t.key.map(f => JString(f)).toList))
        JsonMethods.compact(jv)
      }
    }
  }

  def matrixTableType(username: String, s: String): Response = {
    statusForException {
      userContext(username) { ctx =>
        val x = IRParser.parse_matrix_ir(ctx, s)
        val t = x.typ
        val jv = JObject("global" -> JString(t.globalType.toString),
          "col" -> JString(t.colType.toString),
          "col_key" -> JArray(t.colKey.map(f => JString(f)).toList),
          "row" -> JString(t.rowType.toString),
          "row_key" -> JArray(t.rowKey.map(f => JString(f)).toList),
          "entry" -> JString(t.entryType.toString))
        JsonMethods.compact(jv)
      }
    }
  }

  def blockMatrixType(username: String, s: String): Response = {
    statusForException {
      userContext(username) { ctx =>
        val x = IRParser.parse_blockmatrix_ir(ctx, s)
        val t = x.typ
        val jv = JObject("element_type" -> JString(t.elementType.toString),
          "shape" -> JArray(t.shape.map(s => JInt(s)).toList),
          "is_row_vector" -> JBool(t.isRowVector),
          "block_size" -> JInt(t.blockSize))
        JsonMethods.compact(jv)
      }
    }
  }

  def execute(username: String, sessionID: String, billingProject: String, bucket: String, code: String): Response = {
    statusForException {
      userContext(username) { ctx =>
        ctx.backendContext = new ServiceBackendContext(username, sessionID, billingProject, bucket)

        var x = IRParser.parse_value_ir(ctx, code)
        x = LoweringPipeline.darrayLowerer(true)(DArrayLowering.All).apply(ctx, x)
          .asInstanceOf[IR]
        val (pt, f) = Compile[AsmFunction1RegionLong](ctx,
          FastIndexedSeq[(String, PType)](),
          FastIndexedSeq[TypeInfo[_]](classInfo[Region]), LongInfo,
          MakeTuple.ordered(FastIndexedSeq(x)),
          optimize = true)

        val a = f(0, ctx.r)(ctx.r)
        val v = new UnsafeRow(pt.asInstanceOf[PBaseStruct], ctx.r, a)

        JsonMethods.compact(
          JObject(List("value" -> JSONAnnotationImpex.exportAnnotation(v.get(0), x.typ),
            "type" -> JString(x.typ.toString))))
      }
    }
  }

  def lowerDistributedSort(ctx: ExecuteContext, stage: TableStage, sortFields: IndexedSeq[SortField], relationalLetsAbove: Map[String, IR]): TableStage = {
    // Use a local sort for the moment to enable larger pipelines to run
    LowerDistributedSort.localSort(ctx, stage, sortFields, relationalLetsAbove)
  }

  def persist(backendContext: BackendContext, id: String, value: BlockMatrix, storageLevel: String): Unit = ???

  def unpersist(backendContext: BackendContext, id: String): Unit = ???

  def getPersistedBlockMatrix(backendContext: BackendContext, id: String): BlockMatrix = ???

  def getPersistedBlockMatrixType(backendContext: BackendContext, id: String): BlockMatrixType = ???
}
