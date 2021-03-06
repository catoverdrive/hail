package is.hail.expr.ir

import scala.collection.mutable

object RefEquality {
  def apply[T <: AnyRef](t: T): RefEquality[T] = new RefEquality[T](t)
}

class RefEquality[T <: AnyRef](val t: T) {
  override def equals(obj: scala.Any): Boolean = obj match {
    case r: RefEquality[T] => t.eq(r.t)
    case _ => false
  }

  override def hashCode(): Int = System.identityHashCode(t)

  override def toString: String = s"RefEquality($t)"
}

object Memo {
  def empty[T]: Memo[T] = new Memo[T](new mutable.HashMap[RefEquality[BaseIR], T])
}

class Memo[T] private(val m: mutable.HashMap[RefEquality[BaseIR], T]) {
  def bind(ir: BaseIR, t: T): Memo[T] = {
    val re = RefEquality(ir)
    if (m.contains(re))
      throw new RuntimeException(s"IR already in memo: $ir")
    m += re -> t
    this
  }

  def contains(ir: BaseIR): Boolean = m.contains(RefEquality(ir))

  def lookup(ir: BaseIR): T = m(RefEquality(ir))

  def apply(ir: BaseIR): T = lookup(ir)

  def update(ir: BaseIR, t: => T): Unit = m.update(RefEquality(ir), t)

  def get(ir: BaseIR): Option[T] = m.get(RefEquality(ir))

  def getOrElse(ir: BaseIR, default: => T): T = m.getOrElse(RefEquality(ir), default)

  def getOrElseUpdate(ir: BaseIR, t: => T): T = m.getOrElseUpdate(RefEquality(ir), t)

}

