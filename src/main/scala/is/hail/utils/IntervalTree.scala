package is.hail.utils

import is.hail.annotations.{ExtendedOrdering, Region, RegionValue, RegionValueBuilder}
import is.hail.check._
import is.hail.expr.types.{TBoolean, TInterval, Type}
import org.json4s.JValue
import org.json4s.JsonAST.JObject

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class BaseInterval[T: ClassTag] extends Serializable {
  def start: T

  def end: T

  def includeStart: Boolean

  def includeEnd: Boolean

  def copy(start: T = start, end: T = end, includeStart: Boolean = includeStart, includeEnd: Boolean = includeEnd): BaseInterval[T]

  def contains(pord: ExtendedOrdering, position: T): Boolean = {
    val compareStart = pord.compare(start, position)
    val compareEnd = pord.compare(end, position)
    (compareStart < 0 || (includeStart && compareStart == 0)) && (compareEnd > 0 || (includeEnd && compareEnd == 0))
  }

  def overlaps(pord: ExtendedOrdering, other: BaseInterval[T]): Boolean = {
    (this.contains(pord, other.start) && (other.includeStart || !pord.equiv(this.end, other.start))) ||
      (other.contains(pord, this.start) && (this.includeStart || !pord.equiv(other.end, this.start)))
  }

  // true indicates definitely-empty interval, but false does not guarantee
  // non-empty interval in the (a, b) case;
  // e.g. (1,2) is an empty Interval(Int32), but we cannot guarantee distance
  // like that right now.
  def isEmpty(pord: ExtendedOrdering): Boolean = if (includeStart && includeEnd) pord.gt(start, end) else pord.gteq(start, end)
}

case class Interval(start: Any, end: Any, includeStart: Boolean, includeEnd: Boolean) extends BaseInterval[Any] {

  def copy(start: Any = start, end: Any = end, includeStart: Boolean = includeStart, includeEnd: Boolean = includeEnd): Interval =
    Interval(start, end, includeStart, includeEnd)

  def toJSON(f: (Any) => JValue): JValue =
    JObject("start" -> f(start),
      "end" -> f(end),
      "includeStart" -> TBoolean().toJSON(includeStart),
      "includeEnd" -> TBoolean().toJSON(includeEnd))


  override def toString: String = (if (includeStart) "[" else "(") + start + "-" + end + (if (includeEnd) "]" else ")")

  def toRegionValueInterval(pType: Type, rvb: Option[RegionValueBuilder]): RegionValueInterval = {
    val iType = TInterval(pType)
    val rvb2 = rvb.getOrElse(new RegionValueBuilder(Region()))
    rvb2.start(iType)
    rvb2.startStruct()
    rvb2.addAnnotation(pType, start)
    rvb2.addAnnotation(pType, end)
    rvb2.addBoolean(includeStart)
    rvb2.addBoolean(includeEnd)
    rvb2.endStruct()
    RegionValueInterval(iType, rvb2.region, rvb2.end())
  }
}

case class RegionValueInterval(iType: TInterval, region: Region, offset: Long) extends BaseInterval[RegionValue] {
  def start: RegionValue = RegionValue(region, iType.loadStart(region, offset))

  def end: RegionValue = RegionValue(region, iType.loadEnd(region, offset))

  def includeStart: Boolean = region.loadBoolean(iType.representation.loadField(region, offset, 2))

  def includeEnd: Boolean = region.loadBoolean(iType.representation.loadField(region, offset, 3))

  def copy(start: RegionValue = start, end: RegionValue = end, includeStart: Boolean = includeStart, includeEnd: Boolean = includeEnd): RegionValueInterval = {
    val pType = iType.pointType
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(iType)
    rvb.startStruct()
    rvb.addRegionValue(pType, start)
    rvb.addRegionValue(pType, end)
    rvb.addBoolean(includeStart)
    rvb.addBoolean(includeEnd)
    rvb.endStruct()
    RegionValueInterval(iType, region, rvb.end())
  }
}

object Interval {
  def gen[P](pord: ExtendedOrdering, pgen: Gen[P]): Gen[Interval] =
    Gen.zip(pgen, pgen, Gen.coin(), Gen.coin())
      .map { case (x, y, s, e) =>
        if (pord.lt(x, y))
          Interval(x, y, s, e)
        else
          Interval(y, x, s, e)
      }

  def ordering[T](pord: ExtendedOrdering): ExtendedOrdering = new ExtendedOrdering {
    def compareNonnull(x: Any, y: Any, missingGreatest: Boolean): Int = {
      val xi = x.asInstanceOf[BaseInterval[T]]
      val yi = y.asInstanceOf[BaseInterval[T]]

      val c = pord.compare(xi.start, yi.start, missingGreatest)
      if (c != 0)
        return c
      if (xi.includeStart != yi.includeStart)
        return if (xi.includeStart) -1 else 1

      val c2 = pord.compare(xi.end, yi.end, missingGreatest)
      if (c2 != 0)
        return c2
      if (xi.includeEnd != yi.includeEnd)
        if (xi.includeEnd) 1 else -1
      else 0
    }
  }

  //minimum interval containing this and others.
  def minimumContaining(pord: ExtendedOrdering, ints: Seq[Interval]): Interval = {
    var Interval(s, e, is, ie) = ints.head
    ints.foreach { i =>
      if (pord.lt(i.start, s)) {
        s = i.start
        is = i.includeStart
      } else if (pord.equiv(i.start, s)) is = is || i.includeStart
      if (pord.gt(i.end, e)) {
        e = i.end
        ie = i.includeEnd
      } else if (pord.equiv(i.end, e)) ie = ie || i.includeEnd
    }
    Interval(s, e, is, ie)
  }
}

case class IntervalTree[T: ClassTag, U: ClassTag](root: Option[IntervalTreeNode[T, U]]) extends
  Traversable[(BaseInterval[T], U)] with Serializable {
  override def size: Int = root.map(_.size).getOrElse(0)

  def contains(pord: ExtendedOrdering, position: T): Boolean = root.exists(_.contains(pord, position))

  def overlaps(pord: ExtendedOrdering, interval: BaseInterval[T]): Boolean = root.exists(_.overlaps(pord, interval))

  def queryIntervals(pord: ExtendedOrdering, position: T): Array[BaseInterval[T]] = {
    val b = Array.newBuilder[BaseInterval[T]]
    root.foreach(_.query(pord, b, position))
    b.result()
  }

  def queryValues(pord: ExtendedOrdering, position: T): Array[U] = {
    val b = Array.newBuilder[U]
    root.foreach(_.queryValues(pord, b, position))
    b.result()
  }

  def foreach[V](f: ((BaseInterval[T], U)) => V) {
    root.foreach(_.foreach(f))
  }
}

object IntervalTree {
  def annotationTree[T: ClassTag, U: ClassTag](pord: ExtendedOrdering, values: Array[(BaseInterval[T], U)]): IntervalTree[T, U] = {
    val iord = Interval.ordering[T](pord)
    val sorted = values.sortBy(_._1)(iord.toOrdering.asInstanceOf[Ordering[BaseInterval[T]]])
    new IntervalTree[T, U](fromSorted(pord, sorted, 0, sorted.length))
  }

  def apply[T: ClassTag](pord: ExtendedOrdering, intervals: Array[BaseInterval[T]]): IntervalTree[T, Unit] = {
    val iord = Interval.ordering(pord)
    val sorted = if (intervals.nonEmpty) {
      val unpruned = intervals.sorted(iord.toOrdering.asInstanceOf[Ordering[BaseInterval[T]]])
      val ab = new ArrayBuilder[BaseInterval[T]](intervals.length)
      var tmp = unpruned.head
      var i = 1
      var pruned = 0
      while (i < unpruned.length) {
        val interval = unpruned(i)
        val c = pord.compare(interval.start, tmp.end)
        if (c < 0 || (c == 0 && (interval.includeStart || tmp.includeEnd))) {
          tmp = if (pord.lt(interval.end, tmp.end))
            tmp
          else if (pord.equiv(interval.end, tmp.end))
            tmp.copy(includeEnd = tmp.includeEnd || interval.includeEnd)
          else
            tmp.copy(end = interval.end, includeEnd = interval.includeEnd)
          pruned += 1
        } else {
          ab += tmp
          tmp = interval
        }

        i += 1
      }
      ab += tmp

      ab.result()
    } else intervals

    new IntervalTree[T, Unit](fromSorted(pord, sorted.map(i => (i, ())), 0, sorted.length))
  }

  def fromSorted[T: ClassTag, U](pord: ExtendedOrdering, intervals: Array[(BaseInterval[T], U)], start: Int, end: Int): Option[IntervalTreeNode[T, U]] = {
    if (start >= end)
      None
    else {
      val mid = (start + end) / 2
      val (i, v) = intervals(mid)
      val lft = fromSorted(pord, intervals, start, mid)
      val rt = fromSorted(pord, intervals, mid + 1, end)
      Some(IntervalTreeNode(i, lft, rt, {
        val min1 = lft.map(x => pord.min(x.minimum, i.start).asInstanceOf[T]).getOrElse(i.start)
        rt.map(x => pord.min(x.minimum, min1).asInstanceOf[T]).getOrElse(min1)
      },
        {
          val max1 = lft.map(x => pord.max(x.maximum, i.end).asInstanceOf[T]).getOrElse(i.end)
          rt.map(x => pord.max(x.maximum, max1).asInstanceOf[T]).getOrElse(max1)
        }, v))
    }
  }

  def gen[T](pord: ExtendedOrdering, pgen: Gen[T]): Gen[IntervalTree[Any, Unit]] = {
    Gen.buildableOf[Array](Interval.gen(pord, pgen)).map(a => IntervalTree.apply(pord, a.asInstanceOf[Array[BaseInterval[Any]]]))
  }
}

case class IntervalTreeNode[T: ClassTag, U](i: BaseInterval[T],
  left: Option[IntervalTreeNode[T, U]],
  right: Option[IntervalTreeNode[T, U]],
  minimum: T, maximum: T, value: U) extends Traversable[(BaseInterval[T], U)] {

  override val size: Int =
    left.map(_.size).getOrElse(0) + right.map(_.size).getOrElse(0) + 1

  def contains(pord: ExtendedOrdering, position: T): Boolean = {
    pord.lteq(minimum, position) && pord.gteq(maximum, position) &&
      (left.exists(_.contains(pord, position)) ||
        (pord.lteq(i.start, position) &&
          (i.contains(pord, position) ||
            right.exists(_.contains(pord, position)))))
  }

  def overlaps(pord: ExtendedOrdering, interval: BaseInterval[T]): Boolean = {
    pord.gteq(interval.end, minimum) && pord.lteq(interval.start, maximum) &&
      (left.exists(_.overlaps(pord, interval))) ||
      i.overlaps(pord, interval) || (right.exists(_.overlaps(pord, interval)))
  }

  def query(pord: ExtendedOrdering, b: mutable.Builder[BaseInterval[T], _], position: T) {
    if (pord.lteq(minimum, position) && pord.gteq(maximum, position)) {
      left.foreach(_.query(pord, b, position))
      if (pord.lteq(i.start, position)) {
        right.foreach(_.query(pord, b, position))
        if (i.contains(pord, position))
          b += i
      }
    }
  }

  def queryValues(pord: ExtendedOrdering, b: mutable.Builder[U, _], position: T) {
    if (pord.lteq(minimum, position) && pord.gteq(maximum, position)) {
      left.foreach(_.queryValues(pord, b, position))
      if (pord.lteq(i.start, position)) {
        right.foreach(_.queryValues(pord, b, position))
        if (i.contains(pord, position))
          b += value
      }
    }
  }

  def foreach[V](f: ((BaseInterval[T], U)) => V) {
    left.foreach(_.foreach(f))
    f((i, value))
    right.foreach(_.foreach(f))
  }
}
