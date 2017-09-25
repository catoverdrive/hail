package is.hail.io

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr._
import is.hail.sparkextras.OrderedRDD2
import is.hail.utils._
import is.hail.variant._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.io.Source

object LoadMatrix {

  def warnDuplicates(ids: Array[String]) {
    val duplicates = ids.counter().filter(_._2 > 1)
    if (duplicates.nonEmpty) {
      warn(s"Found ${duplicates.size} duplicate ${plural(duplicates.size, "sample ID")}:\n  @1",
        duplicates.toArray.sortBy(-_._2).map { case (id, count) => s"""($count) "$id"""" }.truncatable("\n  "))
    }
  }

  def placeholderCorrectnessFunction(str: String): Boolean = {
    // if there is anything that we want to check for on a per-line basis
    // check for number of elements in row? Do something else?
    true
  }

  // this assumes that col IDs are in last line of header.
  /// FIXME: Is the toString.split call too slow?
  def parseHeader(lines: Array[String], sep: String = "\t"): Array[String] = {
    lines.last.toString.split(sep)
  }

  def getHeaderLines[T](hConf: Configuration, file: String, nLines: Int = 1): Array[String] = hConf.readFile(file) { s =>
    Source.fromInputStream(s)
      .getLines()
      .take(nLines)
      .toArray
  }

  def apply(hc: HailContext,
            file1: String,
            files: Array[String],
            nPartitions: Option[Int] = None,
            dropSamples: Boolean = false,
            sep: String = "\t"):
  VariantSampleMatrix[Annotation, Annotation, Annotation] = {
    val sc = hc.sc
    val hConf = hc.hadoopConf

    val header1 = parseHeader(getHeaderLines(hConf, file1), sep)
    val header1Bc = sc.broadcast(header1)

    val sampleIds: Array[String] =
      if (dropSamples)
        Array.empty
      else
        header1

    val nSamples = sampleIds.length

    LoadMatrix.warnDuplicates(sampleIds)

    val lines = sc.textFilesLines(files, nPartitions.getOrElse(sc.defaultMinPartitions))

    val fileByPartition = lines.partitions.map(p => partitionPath(p))
    val firstPartitions = fileByPartition.zipWithIndex.filter((name) => name._2 == 0 || fileByPartition(name._2-1) != name._1).map((name) => name._2)

    val matrixType = MatrixType(VSMMetadata(
      sSignature = TString,
      vSignature = TString,
      genotypeSignature = TInt64
    ))

    val keyType = matrixType.kType
    val rowKeys: RDD[RegionValue] = lines.mapPartitionsWithIndex { (i,it) =>

      if (firstPartitions.contains(i)) {
        val hd1 = header1Bc.value
        val hd = it.next().value.split(sep)
        if (!hd1.sameElements(hd)) {
          hd1.zipAll(hd, None, None).zipWithIndex.foreach { case ((s1, s2), j) =>
            if (s1 != s2) {
              fatal(
                s"""invalid sample ids: expected sample ids to be identical for all inputs. Found different sample ids at position $j.
                   |    ${files(0)}: $s1
                   |    ${fileByPartition(i)}: $s2""".
                  stripMargin)
            }
          }
        }
      }

      val region = MemoryBuffer()
      val rvb = new RegionValueBuilder(region)
      val rv = RegionValue(region)

      new Iterator[RegionValue] {
        var present = false

        def advance() {
          while (!present && it.hasNext) {
            it.next().foreach { line =>
              if (line.nonEmpty && placeholderCorrectnessFunction(line)) {
                val k = line.substring(0,line.indexOf(sep))
                region.clear()
                rvb.start(keyType)
                rvb.startStruct()
                rvb.addString(k)
                rvb.addString(k)
                rvb.endStruct()
                rv.setOffset(rvb.end())

                present = true
              }
            }
          }
        }

        def hasNext: Boolean = {
          if (!present)
            advance()
          present
        }

        def next(): RegionValue = {
          hasNext
          assert(present)
          present = false
          rv
        }
      }
    }

    val rdd = lines
      .mapPartitionsWithIndex { (i,it) =>

        val region = MemoryBuffer()
        val rvb = new RegionValueBuilder(region)
        val rv = RegionValue(region, 0)

        if (firstPartitions.contains(i)) {it.next()}

        new Iterator[RegionValue] {
          var present = false

          def advance() {
            while (!present && it.hasNext) {
              it.next().foreach { line =>
                if (line.nonEmpty && placeholderCorrectnessFunction(line)) {

                  val row = line.split(sep)
                  if (nSamples != 0 && row.length != (nSamples + 1)) {
                    fatal(
                      s"""Incorrect number of elements in line:
                         |     There are $nSamples column IDs but ${row.length} elements in line, including row ID.""".stripMargin)
                  }

                  region.clear()
                  rvb.start(matrixType.orderedRDD2Type.rowType.fundamentalType)
                  rvb.startStruct()
                  rvb.addString(row.head)
                  rvb.addString(row.head)
                  rvb.startStruct()
                  rvb.endStruct()

                  rvb.startArray(nSamples)
                  if (nSamples > 0) {
                    for (v <- row.tail) {
                      rvb.addLong(v.trim.toLong)
                    }
                  }
                  rvb.endArray()
                  rvb.endStruct()
                  rv.setOffset(rvb.end())

                  present = true
                }
              }
            }
          }

          def hasNext: Boolean = {
            if (!present)
              advance()
            present
          }

          def next(): RegionValue = {
            if (!present)
              advance()
            assert(present)
            present = false
            rv
          }
        }
      }

    new VariantSampleMatrix(hc,
      VSMMetadata(TString, vSignature = TString, genotypeSignature = TInt64),
      VSMLocalValue(Annotation.empty,
        sampleIds,
        Annotation.emptyIndexedSeq(sampleIds.length)),
      OrderedRDD2(matrixType.orderedRDD2Type, rdd, Some(rowKeys), None))
  }
}
