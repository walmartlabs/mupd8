package com.walmartlabs.mupd8

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach

import java.io.PrintWriter
import java.io.IOException
import java.io.File
import java.util.NoSuchElementException

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class JSONSourceTest extends FunSuite with BeforeAndAfterEach {

  var oneLineFileSource : JSONSource = _
  var emptyFileSource : JSONSource = _

  var oneLineFile : File = _
  var emptyFile : File = _

  val keyAttr = "k1"
  val key = "9"
  val value = "{ \"" + keyAttr + "\" : \"" + key + "\" }"

  override def beforeEach() {
    oneLineFile = new File("oneLineFileSource.data")
    emptyFile = new File("emptyFile.data")

    var writer = new PrintWriter(oneLineFile)
    writer.write(value + "\n")
    writer.flush
    writer.close

    writer = new PrintWriter(emptyFile)
    writer.close

    oneLineFileSource = new JSONSource(List("file:" + oneLineFile.getCanonicalFile, "k1").asJava)
    emptyFileSource = new JSONSource(List("file:" + emptyFile.getCanonicalFile, "k1").asJava)
  }

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
    assert(dataPair != null, "dataPair should not be null reference")
    assert(dataPair._key === key, "dataPair key should be consistent")
    assert(dataPair._value === value.getBytes, "dataPair value should be consistent")
    assert(oneLineFileSource.hasNext == false, "Should consume next item")
  }

  test("getNextDataPair() should throw NoSuchElementException if no more items") {
    intercept[NoSuchElementException] {
      emptyFileSource.getNextDataPair
    }
  }

  override def afterEach() {
    oneLineFile.delete
    emptyFile.delete
  }
}
