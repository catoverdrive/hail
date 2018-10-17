package is.hail.cxx

import is.hail.nativecode._
import is.hail.utils.ArrayBuilder

class TranslationUnit(preamble: String, definitions: Array[Definition]) {

  val functions: Map[String, Function] =
    definitions
      .filter(_.isInstanceOf[Function])
      .map(f => (f.name, f.asInstanceOf[Function]))
      .toMap

  def source: String =
    s"""$preamble
       |
       |NAMESPACE_HAIL_MODULE_BEGIN
       |
       |${definitions.map(_.define).mkString("\n\n")}
       |
       |NAMESPACE_HAIL_MODULE_END
     """.stripMargin

  def build(options: String): NativeModule = {
    val st = new NativeStatus()
    val mod = new NativeModule("", source)
    mod.findOrBuild(st)
    assert(st.ok, st.toString())
    mod
  }
}

class TranslationUnitBuilder() {

  val definitions: ArrayBuilder[Definition] = new ArrayBuilder[Definition]()

  val includes: ArrayBuilder[String] = new ArrayBuilder[String]()

  def include(header: String): Unit = {
    if (header.startsWith("<") && header.endsWith(">"))
      includes += s"#include $header"
    else
      includes += s"""#include "$header""""
  }



  def +=(definition: Definition): Unit =
    definitions += definition

  def result(): TranslationUnit =
    new TranslationUnit(
      includes.result().mkString("\n"),
      definitions.result())
}