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

package com.walmartlabs.mupd8

import java.net.URL
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.json.simple._
import com.walmartlabs.mupd8.Mupd8Main._
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.GT._
import java.util.ArrayList
import java.util.Arrays
import scala.collection.JavaConversions

@RunWith(classOf[JUnitRunner])
class TestAppTest extends FunSuite {

  val cfgDir = this.getClass().getClassLoader().getResource("testapp").getPath()
  val numThreads = 6
  val to = "T10Source"
  val from = "file:" + cfgDir + "/T10.data"
  val sc = "com.walmartlabs.mupd8.JSONSource"
  val sp = "file:" + cfgDir + "/T10.data" + "," + "K1"
  val key = "k1"
  val paras = "-host localhost -pidFile " + cfgDir + "/testapp.pid" + " -key k1 -from file:" + cfgDir + "/T10.data -to T10Source -threads 6 -d " + cfgDir
  val appStaticInfo = new AppStaticInfo(Some(cfgDir), None, None)
  val keyCombo : (String, Key) = ("K1Updater", Key("1".map(_.toByte).toArray))
  val httpStatus = "http://localhost:" + appStaticInfo.statusPort + "/app/status"
  val k1Slate = "http://localhost:" + appStaticInfo.statusPort  + "/app/slate/TestApp/K1Updater/1"

  def getURL(spec : String) : String = {
    val url = new URL(spec)
    val conn = url.openConnection
    conn.connect
    scala.io.Source.fromInputStream(conn.getInputStream).getLines().mkString("\n")
  }

  test("TestApp with NullPool") {
    println("TestApp with NullPool")
    // useNullPool == true
    val appRuntime = new AppRuntime(0, numThreads, appStaticInfo, true)
    // start the source
    appRuntime.startSource("testsource", to, sc, JavaConversions.seqAsJavaList(sp.split(',')))
    Thread.sleep(5000)
    val slate = getURL(k1Slate)
    assert(JSONValue.parse(slate).asInstanceOf[JSONObject].get("counter") == 1000)
    appRuntime.slateURLserver.stop
  }

  test("TestApp with cassandra in the same jvm") {
    println("TestApp with cassandra in the same jvm")

    EmbeddedCassandraServer.start(appStaticInfo.cassKeySpace, appStaticInfo.cassColumnFamily)

    assert(appStaticInfo.cassPort == 19170)
    Thread.sleep(5000)

    val appRuntime = new AppRuntime(0, numThreads, appStaticInfo, false)
    // start the source
    appRuntime.startSource("testsource", to, sc, JavaConversions.seqAsJavaList(sp.split(',')))
    Thread.sleep(30000)
    val slate = getURL(k1Slate)
    assert(JSONValue.parse(slate).asInstanceOf[JSONObject].get("counter") == 1000)

    EmbeddedCassandraServer.stop
  }

}
