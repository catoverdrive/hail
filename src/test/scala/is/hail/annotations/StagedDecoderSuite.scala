package is.hail.annotations

import is.hail.SparkSuite
import is.hail.expr._
import is.hail.utils._
import org.testng.annotations.{BeforeMethod, Test}
import is.hail.annotations.StagedDecoder._

class StagedDecoderSuite extends SparkSuite {

  val region = MemoryBuffer()
  val region2 = MemoryBuffer()
  val rv1 = RegionValue(region)
  val rv2 = RegionValue(region2)
  val rvb = new RegionValueBuilder(region)

  val verbose = false
  val staged = true

  @BeforeMethod def before() {
    region.clear()
    region2.clear()
    region.appendBytes(Array.fill[Byte](100)(0))
    region2.appendBytes(Array.fill[Byte](100)(0))
    region.clear()
    region2.clear()
  }

  def printRegionValue(region:MemoryBuffer, string:String) {
    println(string)
    val size = region.size
    println("Region size: "+size.toString)
    val bytes = region.loadBytes(0,size.toInt)
    println("Array: ")
    var j = 0
    for (i <- bytes) {
      j += 1
      print(i)
      if (j % 30 == 0) {
        print('\n')
      } else {
        print('\t')
      }
    }
    print('\n')
  }

  def checkDecoding(t: Type) {
    if (verbose) {
      printRegionValue(rv1.region, "1")
      println(rv1.pretty(t))
    }
    val en = new Encoder()
    val dec = new Decoder()
    en.clear()
    en.writeRegionValue(t, rv1.region, rv1.offset)
    val (mem, n) = (en.outMem, en.outOff)

    region2.clear()
    dec.set(mem)
    if (staged) {
      t match {
        case t2: TArray =>
          val readArray = dec.getArrayReader(t2)
          rv2.setOffset(readArray(region2))
        case t2: TStruct =>
          val readStruct = dec.getStructReader(t2)
          rv2.setOffset(readStruct(region2))
      }
    } else {
      rv2.setOffset(dec.readRegionValue(t, region2))
    }

    if (verbose) {
      printRegionValue(rv2.region, "2")
      println(rv2.pretty(t))
    }
    assert(rv1.pretty(t) == rv2.pretty(t))

  }

  @Test def decodeArray() {
    val t = TArray(TString)

    rvb.start(t)
    rvb.startArray(2)
    rvb.addString("hello")
    rvb.addString("world")
    rvb.endArray()
    rv1.setOffset(rvb.end())
    checkDecoding(t)

  }

  @Test def decodeStruct() {
    val t = TStruct("a"->TString, "b"->TArray(TInt32))

    rvb.start(t)
    rvb.startStruct()
    rvb.addString("hello")
    rvb.startArray(2)
    rvb.addInt(1)
    rvb.addInt(2)
    rvb.endArray()
    rvb.endStruct()
    rv1.setOffset(rvb.end())
    checkDecoding(t)

  }

  @Test def decodeArrayOfStruct() {
    val t = TArray(TStruct("a"->TString, "b"->TInt32))

    val strVals = Array[String]("hello", "world", "foo", "bar")

    rvb.start(t)
    rvb.startArray(2)
    for (i <- 0 until 2) {
      rvb.startStruct()
      rvb.addString(strVals(i))
      rvb.addInt(i+1)
      rvb.endStruct()
    }
    rvb.endArray()
    rv1.setOffset(rvb.end())

    checkDecoding(t)
  }

  def performanceComparison1(nCols: Int, nIter: Int): (Long, Long) = {
    val t = TStruct("a" -> TString, "b" -> TArray(TInt32))
    val nCols = 100000
    val nIter = 1000

    rvb.start(t)
    rvb.startStruct()
    rvb.addString("row1")
    rvb.startArray(nCols)
    for (i <- 1 to nCols) {
      rvb.addInt(i)
    }
    rvb.endArray()
    rvb.endStruct()
    rv1.setOffset(rvb.end())

    if (verbose) {
      printRegionValue(rv1.region, "1")
      println(rv1.pretty(t))
    }

    val en = new Encoder()
    en.clear()
    en.writeRegionValue(t, rv1.region, rv1.offset)
    val (mem, n) = (en.outMem, en.outOff)

    val dec1 = new Decoder()

    val start1 = System.nanoTime()
    for (i <- 0 until nIter) {
      region2.clear()
      dec1.set(mem)
      rv2.setOffset(dec1.readRegionValue(t, region2))
    }
    val stop1 = System.nanoTime()


    val dec2 = new Decoder()
    val readStruct = dec2.getStructReader(t)

    val start2 = System.nanoTime()
    for (i <- 0 until nIter) {
      region2.clear()
      dec2.set(mem)
      rv2.setOffset(readStruct(region2))
    }
    val stop2 = System.nanoTime()

    (stop1 - start1, stop2 - start2)
  }

  def performanceComparison2(nCols: Int, nIter: Int): (Long, Long) = {
    val t = TArray(TStruct("a" -> TString, "b" -> TInt32))
    val nCols = 100000
    val nIter = 1000

    rvb.start(t)
    rvb.startArray(nCols)
    for (i <- 1 to nCols) {
      rvb.startStruct()
      rvb.addString("row1")
      rvb.addInt(i)
      rvb.endStruct()
    }
    rvb.endArray()
    rv1.setOffset(rvb.end())

    if (verbose) {
      printRegionValue(rv1.region, "1")
      println(rv1.pretty(t))
    }

    val en = new Encoder()
    en.clear()
    en.writeRegionValue(t, rv1.region, rv1.offset)
    val (mem, n) = (en.outMem, en.outOff)

    val dec1 = new Decoder()

    val start1 = System.nanoTime()
    for (i <- 0 until nIter) {
      region2.clear()
      dec1.set(mem)
      rv2.setOffset(dec1.readRegionValue(t, region2))
    }
    val stop1 = System.nanoTime()


    val dec2 = new Decoder()
    val readStruct = dec2.getArrayReader(t)

    val start2 = System.nanoTime()
    for (i <- 0 until nIter) {
      region2.clear()
      dec2.set(mem)
      rv2.setOffset(readStruct(region2))
    }
    val stop2 = System.nanoTime()

    (stop1 - start1, stop2 - start2)
  }

  @Test def testPerformance() {
    val nIter = 1000

    printf("nCols   |  delta1  | percent1 |  delta2  | percent2%n")
    for (i <- Array(1000, 10000, 100000)) {
      val (old1, new1) = performanceComparison1(i, nIter)
      val (old2, new2) = performanceComparison2(i, nIter)
      printf("%7d | %7.5f | %7.5f | %7.5f | %7.5f %n", i, (new1-old1)/1000000000.0, (new1 - old1).toDouble/old1, (new2-old2)/1000000000.0, (new2 - old2).toDouble/old2)
    }
  }
}
