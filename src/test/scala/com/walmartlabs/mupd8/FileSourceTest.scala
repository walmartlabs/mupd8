package com.walmartlabs.mupd8

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach

import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._

import java.io.BufferedReader
import java.lang.RuntimeException
import java.io.IOException
import java.util.Random

@RunWith(classOf[JUnitRunner])
class FileSourceTest extends FunSuite with BeforeAndAfterEach with MockitoSugar {
  
  val sourceMock = mock[JSONSource]
  val readerMock = mock[BufferedReader]

  val Line = "foo"

  override def beforeEach() {
    when(sourceMock._reader).thenReturn(readerMock)
  }

  test("Should return 'None' when failed to read") {
    when(sourceMock._reconnectOnEof).thenReturn(false)
    when(readerMock.readLine).thenThrow(new RuntimeException()).thenReturn(Line)
    
    intercept[IOException] {
      sourceMock._readLine()
    }
  }
}
