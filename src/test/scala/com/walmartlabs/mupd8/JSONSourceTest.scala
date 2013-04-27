package com.walmartlabs.mupd8

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import java.io.PrintWriter
import java.io.IOException
import java.io.File
import java.util.NoSuchElementException

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class JSONSourceTest extends FunSuite with BeforeAndAfter {

  var oneLineFileSource : JSONSource = _
  var emptyFileSource : JSONSource = _

  val oneLineFile = new File("oneLineFileSource.data")
  val emptyFile = new File("emptyFile.data")

  val keyAttr = "k1"
  val key = "9"
  val value = "{ \"" + keyAttr + "\" : \"" + key + "\" }"

  before {
    try {
      var writer = new PrintWriter(oneLineFile)
      writer.write(value + "\n")
      writer.flush
      writer.close

      writer = new PrintWriter(emptyFile)
    } catch {
      case e: IOException => { println("Failed to create test input file"); fail }
    }
    oneLineFileSource = new JSONSource(List("file:" + oneLineFile.getCanonicalFile, "k1").asJava)
    emptyFileSource = new JSONSource(List("file:" + emptyFile.getCanonicalFile, "k1").asJava)
  }

  /*
   * The following tests are somewhat generic to Mupd8Source interface
   */

  test("hasNext() should return true when there's next item") {
    assert(oneLineFileSource.hasNext == true)
  }

  test("hasNext() should not consume next item") {
    assert(oneLineFileSource.hasNext == true)
    assert(oneLineFileSource.hasNext == true)
  }  

  test("hasNext() should return false when there's no more item") {
    assert(emptyFileSource.hasNext == false)
  }

  test("getNextDataPair() should consume the next item and return valid data pair") {
    val dataPair = oneLineFileSource.getNextDataPair
    assert(dataPair != null, "dataPair is null reference")
    assert(dataPair._key === key, "dataPair key is inconsistent")
    assert(dataPair._value === value.getBytes, "dataPair value is inconsistent")
    assert(oneLineFileSource.hasNext == false, "Doesn't consume next item")
  }

  test("getNextDataPair() should throw NoSuchElementException if no more items") {
    intercept[NoSuchElementException] {
      emptyFileSource.getNextDataPair
    }
  }

  /*
   * The following tests are specific to JSONSource
   */

  after {
    oneLineFile.delete
    emptyFile.delete
  }
}
