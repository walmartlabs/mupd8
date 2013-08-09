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
import org.junit.Assert._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

import org.scale7.cassandra.pelops._

import com.walmartlabs.mupd8.Mupd8Main._
import com.walmartlabs.mupd8.examples._

@RunWith(classOf[JUnitRunner])
class AppInfoTest extends FunSuite {
  val cfgDir = this.getClass().getClassLoader().getResource("testapp").getPath()
  val appInfo = new AppStaticInfo(Some(cfgDir), None, None)

  test("test ttl extraction") {
    val k3 = "K3Updater"
    val k3TTL = appInfo.performers(appInfo.performerName2ID(k3)).ttl
    assertTrue(k3TTL == Mutator.NO_TTL)
    val k4 = "K4Updater"
    val k4TTL = appInfo.performers(appInfo.performerName2ID(k4)).ttl
    assertTrue(k4TTL == 300)
  }
}
