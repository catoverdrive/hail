package is.hail.services.batch_client

import java.io.FileInputStream

import is.hail.utils._
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST._
import org.json4s.jackson.Serialization

import scala.collection.mutable

object JavaBatchSpec {
  def fromFile(localFile: String): JavaBatchSpec = {
    using(new FileInputStream(localFile)) { is =>
      implicit val formats: Formats = DefaultFormats
      Serialization.read[JavaBatchSpecParams](is).asBatchSpec()
    }
  }

  def fromString(jsonString: String): JavaBatchSpec = {
    implicit val formats: Formats = DefaultFormats
    Serialization.read[JavaBatchSpecParams](jsonString).asBatchSpec()
  }
}

case class JavaBatchSpecParams(
  name: Option[String],
  project: String,
  image: String,
  token: Option[String],
  jobs: IndexedSeq[JavaJobSpecParams]) {
  def asBatchSpec(): JavaBatchSpec =
    JavaBatchSpec(name.getOrElse(s"JavaBatchExecutor_${ tokenUrlSafe(5) }"), project, token.getOrElse(tokenUrlSafe(32)), jobs.map(_.asJobSpec(null, image)))
}

case class JavaBatchSpec(name: String, project: String, token: String, jobs: IndexedSeq[JavaJobSpec]) {
  private[this] def parseDependencies(): IndexedSeq[JObject] = {
    val jobSpecs = new ArrayBuilder[JObject]()
    val incomplete = mutable.Set[JavaJobSpec](jobs: _*)
    val idMap = mutable.Map[String, Int]()

    val namedSteps = jobs.filter(_.name != null).map(_.name).toSet

    jobs.zipWithIndex.foreach { case (j, i) =>
      if (j.name != null && namedSteps.contains(j.name))
        throw new RuntimeException(s"Found duplicate named step: ${j.name}")
    }

    var changed = false
    while (incomplete.nonEmpty) {
      incomplete.foreach { job =>
        if (job.checkAndSetValidParents(idMap)) {
          val id = jobSpecs.length
          job.setID(id)
          jobSpecs += job.asJValue
          if (job.name != null)
            idMap.update(job.name, id)
          incomplete.remove(job)
          changed = true
        }
      }
      if (!changed)
        throw new RuntimeException(s"Circular dependencies between steps ${ incomplete.map(_.name).mkString(", ") }.")
    }
    jobSpecs.result()
  }

  def run(client: BatchClient): Unit = {
    val jobSpecs = parseDependencies()

    val batch = client.run(
      JObject(
        "name" -> JString(name),
        "billing_project" -> JString(project),
        "n_jobs" -> JInt(jobs.length),
        "token" -> JString(token)),
      jobSpecs)
    implicit val formats: Formats = DefaultFormats
    val batchID = (batch \ "id").extract[Int]
    val batchState = (batch \ "state").extract[String]
    if (batchState != "success")
      throw new RuntimeException(s"batch $batchID failed: $batchState")
  }
}

case class JavaBatchIOFiles(from: String, to: String) {
  def asJValue: JValue = JObject("from" -> JString(from), "to" -> JString(to))
}

case class JavaJobSpecParams(
  name: Option[String],
  flags: Option[IndexedSeq[String]],
  className: String,
  args: Option[IndexedSeq[String]],
  inputFiles: Option[IndexedSeq[JavaBatchIOFiles]],
  outputFiles: Option[IndexedSeq[JavaBatchIOFiles]],
  parents: Option[IndexedSeq[String]]) {
  def asJobSpec(defaultName: String, image: String): JavaJobSpec =
    new JavaJobSpec(
      name.getOrElse(defaultName),
      image,
      flags.getOrElse(FastIndexedSeq()),
      className,
      args.getOrElse(FastIndexedSeq()),
      inputFiles.getOrElse(FastIndexedSeq()),
      outputFiles.getOrElse(FastIndexedSeq()),
      parents.getOrElse(FastIndexedSeq()))
}

class JavaJobSpec(val name: String, image: String, flags: IndexedSeq[String], className: String, args: IndexedSeq[String], inputFiles: IndexedSeq[JavaBatchIOFiles], outputFiles: IndexedSeq[JavaBatchIOFiles], parents: IndexedSeq[String]) {
  def this(name: String, image: String, className: String, args: IndexedSeq[String]) = this(name, image, FastIndexedSeq(), className, args, FastIndexedSeq(), FastIndexedSeq(), FastIndexedSeq())

  private[this] var id: Int = _
  private[this] var parentIds: IndexedSeq[Int] = _

  def checkAndSetValidParents(idMap: mutable.Map[String, Int]): Boolean = {
    val valid = parents.forall(p => idMap.contains(p))
    if (valid)
      parentIds = parents.map(idMap(_))
    valid
  }

  def setID(newID: Int): Unit = id = newID

  def asJValue: JObject = JObject(
    "name" -> (if (name != null) JString(name) else JNull),
    "always_run" -> JBool(false),
    "image" -> JString(image),
    "mount_docker_socket" -> JBool(false),
    "command" -> JArray(List(
      JString("/bin/bash"),
      JString("-c"),
      JString(s"time java ${ flags.mkString(" ") } -cp $$SPARK_HOME/jars/*:/hail.jar $className ${args.mkString(" ")}"))),
    "input_files" -> JArray(inputFiles.map(_.asJValue).toList),
    "output_files" -> JArray(outputFiles.map(_.asJValue).toList),
    "job_id" -> JInt(id),
    "parent_ids" -> JArray(parentIds.map(JInt(_)).toList))
}

object JavaBatchExecutor {
  def main(args: Array[String]): Unit = {
    val spec = args.toFastIndexedSeq match {
      case IndexedSeq(filename) => JavaBatchSpec.fromFile(filename)
      case IndexedSeq("-s", string) => JavaBatchSpec.fromString(string)
      case IndexedSeq("-f", filename) => JavaBatchSpec.fromFile(filename)
    }
    spec.run(new BatchClient())
  }
}
