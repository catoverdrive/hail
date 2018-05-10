package is.hail.expr.ir.functions

import is.hail.expr.ir._
import is.hail.expr.types._

object DictFunctions extends RegistryFunctions {

  def registerAll() {

    registerIR("contains", TDict(tv("T"), tv("U")), tv("T"))(DictContains)

    registerIR("get", TDict(tv("T"), tv("U")), tv("T"))(DictGet)

    registerIR("get", TDict(tv("T"), tv("U")), tv("T"), tv("U")) { //getOrElse
      (dictIR, keyIR, default) =>
        val d = genUID()
        val k = genUID()
        val dict = Ref(d, dictIR.typ)
        val key = Ref(k, keyIR.typ)
        Let(d, dictIR, Let(k, keyIR, If(DictContains(dict, key), DictGet(dict, key), default)))
    }

    registerIR("[]", TDict(tv("T"), tv("U")), tv("T")) {
      (dictIR, keyIR) =>
        val d = genUID()
        val k = genUID()
        val dict = Ref(d, dictIR.typ)
        val key = Ref(k, keyIR.typ)
        Let(d, dictIR, Let(k, keyIR, If(DictContains(dict, key), DictGet(dict, key), Die("can't do the thing!"))))
    }


  }
}
