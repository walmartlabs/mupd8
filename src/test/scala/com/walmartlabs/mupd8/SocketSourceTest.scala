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
import java.util.Random

@RunWith(classOf[JUnitRunner])
class SocketSourceTest extends FunSuite with BeforeAndAfterEach with MockitoSugar {

  val sourceMock = mock[JSONSource]
  val readerMock = mock[BufferedReader]

  val Line = "foo"

  override def beforeEach() {
    when(sourceMock._reconnectOnEof).thenReturn(true)
    when(sourceMock._random).thenReturn(new Random(0))
    when(sourceMock._reader).thenReturn(readerMock)
  }

  test("Should connect to socket during initialization, retry until success") {
    when(sourceMock.socketReader).thenThrow(new RuntimeException()).thenReturn(readerMock)
    
    val expected = readerMock
    val actual = sourceMock._ensureSocketReaderCreated()

    assert(expected eq actual)
  }

  test("Should reconnect to socket if there was any I/O exception during reads") {
    when(readerMock.readLine).thenThrow(new RuntimeException()).thenReturn(Line)
    when(sourceMock._ensureSocketReaderCreated()).thenReturn(readerMock)
    doNothing.when(sourceMock)._ensureReaderClosed
    
    val expected = Line
    val actual = sourceMock._readLine().get

    assert(expected == actual)
  }
}
