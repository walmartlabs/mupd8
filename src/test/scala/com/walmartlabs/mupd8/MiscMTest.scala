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

import java.net.URL
import java.net.InetAddress

import org.junit.runner.RunWith
import org.junit.Assert._
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.xerial.snappy.Snappy

import com.walmartlabs.mupd8.miscM._
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.compression.CompressionFactory
import com.walmartlabs.mupd8.compression.CompressionService

@RunWith(classOf[JUnitRunner])
class MiscMTest extends FunSuite {

  test("localIPAddresses") {
    val localhostIp = InetAddress.getByName("localhost").getHostAddress
    assertTrue("localhost address should exist", localIPAddresses.exists(_ == localhostIp))
  }

  test("CompressionAndUncompression") {
    val testStr = "abcd" * 1000
    val snappy = CompressionFactory.getService("snappy")
    val snappied = snappy.compress(testStr.getBytes)
    val unsnappied = snappy.uncompress(snappied)
    assertTrue("Snappy should not mutate content", testStr == new String(unsnappied))
    
    val gzip = CompressionFactory.getService("gzip")
    val gzipd = gzip.compress(testStr.getBytes)
    val ungziped = gzip.uncompress(gzipd)
    assertTrue("gzip should not mutate content", testStr == new String(ungziped))
    
    val nocomp = CompressionFactory.getService("nocompression")
    val nocomped = nocomp.compress(testStr.getBytes)
    assertTrue("non should not mutate content", testStr == new String(nocomped))
  }

}
