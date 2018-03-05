package is.hail.expr.ir.functions

import is.hail.asm4s._
import is.hail.expr.types._

object UtilFunctions {

  val range: IRFunction[Array[Int]] = IRFunction[Array[Int]]("range", TInt32(), TInt32(), TArray(TInt32())) {
    case Array(start: Code[Int], end: Code[Int]) =>
      Code.invokeStatic[java.util.stream.IntStream, Int, Int, Array[Int]]("range", start, end)
  }

  val triangle: IRFunction[Int] = IRFunction[Int]("triangle", TInt32(), TInt32()) {
    case Array(n: Code[Int]) => (n * (n + 1)) / 2
  }
}
