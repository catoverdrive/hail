package is.hail.sparkextras

import is.hail.annotations._
import is.hail.rvd.{OrderedRVD, OrderedRVDPartitioner, OrderedRVDType}
import is.hail.utils._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class OrderedDependency(left: OrderedRVD, right: OrderedRVD) extends NarrowDependency[RegionValue](right.rdd) {
  override def getParents(partitionId: Int): Seq[Int] =
    OrderedDependency.getDependencies(left.partitioner, right.partitioner)(partitionId)
}

object OrderedDependency {
  def getDependencies(p1: OrderedRVDPartitioner, p2: OrderedRVDPartitioner)(partitionId: Int): Seq[Int] = {
    val partBounds = p1.rangeBounds(partitionId).asInstanceOf[Interval]

    if (!p2.rangeTree.probablyOverlaps(p2.pkType.ordering, partBounds))
      Seq.empty[Int]
    else {
      assert(p2.pkType.typeCheck(partBounds.start), s"p2.pkType: ${ p2.pkType }\n p1.pkType: ${ p1.pkType }\n partBound = ${partBounds.start}")
      info(s"p2.pkType: ${ p2.pkType }\n p1.pkType: ${ p1.pkType }\n partBound = ${partBounds.start}")
      val start = p2.getPartitionPK(partBounds.start)
      assert(p2.pkType.typeCheck(partBounds.end), s"p2.pkType: ${ p2.pkType }\n p1.pkType: ${ p1.pkType }\n partBound = ${partBounds.end}")
      info(s"p2.pkType: ${ p2.pkType }\n p1.pkType: ${ p1.pkType }\n partBound = ${partBounds.end}")
      val end = p2.getPartitionPK(partBounds.end)
      start to end
    }
  }
}

case class OrderedJoinDistinctRDD2Partition(index: Int, leftPartition: Partition, rightPartitions: Array[Partition]) extends Partition

class OrderedJoinDistinctRDD2(left: OrderedRVD, right: OrderedRVD, joinType: String)
  extends RDD[JoinedRegionValue](left.sparkContext,
    Seq[Dependency[_]](new OneToOneDependency(left.rdd),
      new OrderedDependency(left, right))) {
  assert(joinType == "left" || joinType == "inner")
  override val partitioner: Option[Partitioner] = Some(left.partitioner)

  def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](left.getNumPartitions)(i =>
      OrderedJoinDistinctRDD2Partition(i,
        left.partitions(i),
        OrderedDependency.getDependencies(left.partitioner, right.partitioner)(i)
          .map(right.partitions)
          .toArray))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = left.rdd.preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[JoinedRegionValue] = {
    val partition = split.asInstanceOf[OrderedJoinDistinctRDD2Partition]

    val leftIt = left.rdd.iterator(partition.leftPartition, context)
    val rightIt = partition.rightPartitions.iterator.flatMap { p =>
      right.rdd.iterator(p, context)
    }

    joinType match {
      case "inner" => new OrderedInnerJoinDistinctIterator(left.typ, right.typ, leftIt, rightIt)
      case "left" => new OrderedLeftJoinDistinctIterator(left.typ, right.typ, leftIt, rightIt)
      case _ => fatal(s"Unknown join type `$joinType'. Choose from `inner' or `left'.")
    }
  }
}

class OrderedZipJoinIterator(
  leftTyp: OrderedRVDType, rightTyp: OrderedRVDType,
  lit: Iterator[RegionValue], rit: Iterator[RegionValue])
  extends Iterator[JoinedRegionValue] {

  private val lrKOrd = OrderedRVDType.selectUnsafeOrdering(leftTyp.rowType, leftTyp.kRowFieldIdx, rightTyp.rowType, rightTyp.kRowFieldIdx)

  val jrv = JoinedRegionValue()
  var present: Boolean = false

  var lrv: RegionValue = _
  var rrv: RegionValue = _

  def nextLeft() {
    if (lrv == null && lit.hasNext)
      lrv = lit.next()
  }

  def nextRight() {
    if (rrv == null && rit.hasNext)
      rrv = rit.next()
  }

  def hasNext: Boolean = {
    if (!present) {
      nextLeft()
      nextRight()

      val c = {
        if (lrv != null) {
          if (rrv != null)
            lrKOrd.compare(lrv, rrv)
          else
            -1
        } else if (rrv != null)
          1
        else {
          assert(!lit.hasNext && !rit.hasNext)
          return false
        }
      }

      if (c == 0) {
        jrv.rvLeft = lrv
        jrv.rvRight = rrv
        lrv = null
        rrv = null
      } else if (c < 0) {
        jrv.rvLeft = lrv
        lrv = null
        jrv.rvRight = null
      } else {
        // c > 0
        jrv.rvLeft = null
        jrv.rvRight = rrv
        rrv = null
      }
      present = true
    }

    present
  }

  def next(): JoinedRegionValue = {
    if (!hasNext)
      throw new NoSuchElementException("next on empty iterator")
    present = false
    jrv
  }
}

case class OrderedZipJoinRDDPartition(index: Int, leftPartition: Partition, rightPartitions: Array[Partition]) extends Partition

class OrderedZipJoinRDD(left: OrderedRVD, right: OrderedRVD)
  extends RDD[JoinedRegionValue](left.sparkContext,
    Seq[Dependency[_]](new OneToOneDependency(left.rdd),
      new OrderedDependency(left, right))) {

  assert(left.partitioner.pkType.ordering.lteq(left.partitioner.minBound, right.partitioner.minBound) &&
    left.partitioner.pkType.ordering.gteq(left.partitioner.maxBound, right.partitioner.maxBound))

  private val leftPartitionForRightRow = new OrderedRVDPartitioner(
    right.typ.partitionKey,
    right.typ.rowType,
    left.partitioner.rangeBounds)

  override val partitioner: Option[Partitioner] = Some(left.partitioner)

  def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](left.getNumPartitions)(i =>
      OrderedZipJoinRDDPartition(i,
        left.partitions(i),
        OrderedDependency.getDependencies(left.partitioner, right.partitioner)(i)
          .map(right.partitions)
          .toArray))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = left.rdd.preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[JoinedRegionValue] = {
    val partition = split.asInstanceOf[OrderedZipJoinRDDPartition]
    val index = partition.index

    val leftIt = left.rdd.iterator(partition.leftPartition, context)
    val rightIt = partition.rightPartitions.iterator.flatMap { p =>
      right.rdd.iterator(p, context)
    }
      .filter { rrv => leftPartitionForRightRow.getPartition(rrv) == index }

    new OrderedZipJoinIterator(left.typ, right.typ, leftIt, rightIt)
  }
}
