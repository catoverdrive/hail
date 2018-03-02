package is.hail.rvd

import is.hail.annotations._
import is.hail.expr.types._
import is.hail.utils._
import org.apache.spark.Partitioner
import org.apache.spark.sql.Row

class OrderedRVDPartitioner(
  val partitionKey: Array[String], val kType: TStruct,
  // rangeBounds: Array[Interval[pkType]]
  // rangeBounds is interval containing all partition keys within a partition
  val rangeBounds: UnsafeIndexedSeq) extends Partitioner {
  val numPartitions: Int = rangeBounds.length

  val (pkType, _) = kType.select(partitionKey)
  val pkIntervalType = TInterval(pkType)
  val rangeBoundsType = TArray(pkIntervalType)

  assert(rangeBoundsType.typeCheck(rangeBounds))

  require(rangeBounds.isEmpty || (rangeBounds.zip(rangeBounds.tail).forall { case (left: Interval, right: Interval) =>
    !left.probablyOverlaps(pkType.ordering, right) && pkType.ordering.lteq(left.start, right.start)
  } && rangeBounds.forall { case i: Interval =>
    pkType.ordering.lteq(i.start, i.end) && !i.definitelyEmpty(pkType.ordering)
  } ))

  require(rangeBounds.isEmpty || rangeBounds.zip(rangeBounds.tail).forall { case (left: Interval, right: Interval) =>
    pkType.ordering.equiv(left.end, right.start) && (left.includeEnd || right.includeStart) } )

  val rangeTree: IntervalTree[Int] = IntervalTree.fromSorted(pkType.ordering,
    Array.tabulate[(Interval, Int)](numPartitions) { i =>
      (rangeBounds(i).asInstanceOf[Interval], i)
    })

  val pkKFieldIdx: Array[Int] = partitionKey.map(n => kType.fieldIdx(n))

  def minBound: Any = rangeBounds(0).asInstanceOf[Interval].start
  def maxBound: Any = rangeBounds(numPartitions - 1).asInstanceOf[Interval].end

  // if outside bounds, return min or max depending on location
  // pk: Annotation[pkType]
  def getPartitionPK(pk: Any): Int = {
    assert(pkType.typeCheck(pk))
    val part = rangeTree.queryValues(pkType.ordering, pk)
    part match {
      case Array() =>
        if (pkType.ordering.lt(pk, minBound))
          0
        else {
          info(s"pk: $pk")
          info(s"rangeBounds: $rangeBounds")
          assert(pkType.ordering.gt(pk, maxBound), s"pk: $pk; maxBound: $maxBound")
          numPartitions - 1
        }

      case Array(x) => x
    }
  }

  // return the partition containing key
  // if outside bounds, return min or max depending on location
  // key: RegionValue[kType]
  def getPartition(key: Any): Int = {
    val keyrv = key.asInstanceOf[RegionValue]
    val wpkrv = WritableRegionValue(pkType)
    wpkrv.setSelect(kType, pkKFieldIdx, keyrv)
    val pkUR = new UnsafeRow(pkType, wpkrv.value)

    val part = rangeTree.queryValues(pkType.ordering, pkUR)

    part match {
      case Array() =>
        if (pkType.ordering.lt(pkUR, minBound))
          0
        else {
          assert(pkType.ordering.gt(pkUR, maxBound))
          numPartitions - 1
        }
      case Array(x) => x
    }
  }

  def withKType(newPartitionKey: Array[String], newKType: TStruct): OrderedRVDPartitioner = {
    val (newPKType, _) = newKType.select(newPartitionKey)
    val newRangeBounds = new UnsafeIndexedSeq(TArray(TInterval(newPKType)), rangeBounds.region, rangeBounds.aoff)
    val newPart = new OrderedRVDPartitioner(newPartitionKey, newKType, newRangeBounds)
    assert(newPart.pkType.types.sameElements(pkType.types))
    newPart
  }

  def copy(numPartitions: Int = numPartitions, partitionKey: Array[String] = partitionKey,
    kType: TStruct = kType, rangeBounds: UnsafeIndexedSeq = rangeBounds): OrderedRVDPartitioner = {
    new OrderedRVDPartitioner(partitionKey, kType, rangeBounds)
  }

  def coalesceRangeBounds(newPartEnd: Array[Int]): OrderedRVDPartitioner = {
    val newRangeBounds = UnsafeIndexedSeq(
      rangeBoundsType,
      (-1 +: newPartEnd.init).zip(newPartEnd).map { case (s, e) =>
        val i1 = rangeBounds(s + 1).asInstanceOf[Interval]
        val i2 = rangeBounds(e).asInstanceOf[Interval]
        Interval(i1.start, i2.end, i1.includeStart, i2.includeEnd)
      })
    copy(numPartitions = newPartEnd.length, rangeBounds = newRangeBounds)
  }
}

object OrderedRVDPartitioner {
  def empty(typ: OrderedRVDType): OrderedRVDPartitioner = {
    new OrderedRVDPartitioner(typ.partitionKey, typ.kType, UnsafeIndexedSeq.empty(TArray(TInterval(typ.pkType))))
  }

  // takes npartitions + 1 points and returns npartitions intervals: [a,b], (b,c], (c,d], ... (i, j]
  def makeRangeBoundIntervals(pType: Type, rangeBounds: Array[RegionValue]): UnsafeIndexedSeq = {
    val uisRangeBounds = UnsafeIndexedSeq(TArray(pType), rangeBounds)
    var includeStart = true
    val rangeBoundIntervals = uisRangeBounds.zip(uisRangeBounds.tail).map { case (s, e) =>
        val i = Interval(s, e, includeStart, true)
        includeStart = false
        i
    }
    UnsafeIndexedSeq(TArray(TInterval(pType)), rangeBoundIntervals)
  }
}
