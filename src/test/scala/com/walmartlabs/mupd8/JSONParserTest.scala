/**
 * Copyright 2011-2012 @WalmartLabs, a division of Wal-Mart Stores, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.walmartlabs.mupd8;


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

import org.json.simple._

import com.walmartlabs.mupd8._
import com.walmartlabs.mupd8.Misc._

@RunWith(classOf[JUnitRunner])
class JSONParserTest extends FunSuite {

  def extractKey(key : String, event : String) : Option[AnyRef] =
    // the following parsing would fail if event is JSONArray
    excToOptionWithLog {
      key.split(':').foldLeft(JSONValue.parse(event))((js,k) => js.asInstanceOf[JSONObject].get(k).asInstanceOf[Object])
    }

  test( "decoding") {
    val jsonText = "{\"first\": 123, \"second\": [4, 5, 6], \"third\": 789}"
    val jsonObj = JSONValue.parse(jsonText).asInstanceOf[JSONObject]
    assert(jsonObj.get( "first") == 123)
  }

  test( "multi-layer key extraction") {
    val jsonText = "{\"first\": 123, \"second\": [4, 5, 6], \"third\": {\"fourth\": 789}}"
    val key1 = "first"
    val out1 = extractKey(key1, jsonText)
    assert(out1.isDefined)
    assert(out1.getOrElse(0) == 123)
    val key2 = "first1"
    val out2 = extractKey(key2, jsonText)
    assert(out2.orNull == null)
    val key3 = "third:fourth"
    val out3 = extractKey(key3, jsonText)
    assert(out3.isDefined)
    assert(out3.getOrElse(0) == 789)
    assert(out3.getOrElse(0).toString() == "789")
  }

  test("test multi-thread source reader running on one node") {
    val source = new JSONSource(List("file:./src/test/resources/testapp/T10.data", "k1").asJava)
    val source1 = new JSONSource(List("file:./src/test/resources/testapp/T10.data", "k1").asJava)
    val source2 = new JSONSource(List("file:./src/test/resources/testapp/T10.data", "k1").asJava)
    new Thread() {
      override def run() {
        for (i <- 0 until 5000) {
          if (source.hasNext) {
            val dataPair = source.getNextDataPair
            assert(dataPair != null)
            assert(dataPair._key != null)
            assert(dataPair._value != null)
          }
        }
      }
    }.start()
    new Thread() {
      override def run() {
        for (i <- 0 until 5000) {
          if (source1.hasNext) {
            val dataPair = source1.getNextDataPair
            assert(dataPair != null)
            assert(dataPair._key != null)
            assert(dataPair._value != null)
          }
        }
      }
    }.start()
    new Thread() {
      override def run() {
        for (i <- 0 until 5000) {
          if (source2.hasNext) {
            val dataPair = source2.getNextDataPair
            assert(dataPair != null)
            assert(dataPair._key != null)
            assert(dataPair._value != null)
          }
        }
      }
    }.start()
  }
}

