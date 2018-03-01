package is.hail.expr.ir.functions

import java.io.Serializable

import is.hail.asm4s.Code
import is.hail.check.Gen
import is.hail.expr.Parser
import is.hail.expr.types._
import is.hail.utils.{fatal, triangle}
import is.hail.variant.{AllelePair, Call, Call1, Call2, CallN, Genotype}

import scala.annotation.switch
import scala.collection.mutable

object IRFunctionRegistry {
  val registry: mutable.Map[String, IndexedSeq[IRFunction[_]]] = mutable.Map()

  def addFunction(name: String, f: IRFunction[_]) {
    val updated = registry.getOrElse(name, IndexedSeq()) :+ f
    registry.put(name, updated)
  }
}

object IRFunction {
  def apply[T](name: String, t: Type*)(impl: Array[Code[_]] => Code[T]): IRFunction[T] = {
    val f = new IRFunction[T] {
      def types: Array[Type] = t.toArray
      def implementation: Array[Code[_]] => Code[T] = impl
    }
    IRFunctionRegistry.addFunction(name, f)
    f
  }
}

abstract class IRFunction[T] {
  def types: Array[Type]
  def implementation: Array[Code[_]] => Code[T]
  def apply(args: Code[_]*): Code[T] = implementation(args.toArray)
}