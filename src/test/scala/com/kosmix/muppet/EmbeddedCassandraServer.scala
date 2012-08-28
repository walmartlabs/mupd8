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

import java.io.File
import java.io.IOException
import java.util.HashMap
import java.util.ArrayList

import org.apache.log4j._

import org.apache.commons.io.FileUtils
import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.thrift.CfDef
import org.apache.cassandra.thrift.KsDef

import org.scale7.cassandra.pelops.Cluster
import org.scale7.cassandra.pelops.ColumnFamilyManager
import org.scale7.cassandra.pelops.KeyspaceManager

object EmbeddedCassandraServer {

  val baseDir:String = "target/tmp"
  val homeDir:String = "."
  var cassandraDaemon:CassandraDaemon = null
  var cassThread:Thread = null

  def setup : Unit = {
    try {
      val dir:File = new File(baseDir)
      val logCfg:File = new File(this.getClass().getClassLoader().getResource("testapp/log4j.properties").getPath())
      // since 0.7, use cassandra.yaml
      val cassCfg:File = new File(this.getClass().getClassLoader().getResource("testapp/cassandra.yaml").getPath())
      // log4j.properties must be available at classpath !!!
      FileUtils.copyFileToDirectory(logCfg, new File(homeDir))
      System.setProperty("cassandra.config", "file:" + cassCfg);
      System.setProperty("log4j.configuration", "file:" + logCfg);
    } catch {
      case e: IOException => {
        System.err.println("Failed to setup, exit..." + e.getMessage)
        System.exit(1)
      }
    }
  }

  def start(keyspace : String, cf : String) : Unit = {
    println("init and start embedded cassandra...")
    setup
    try {
      // start cassandra daemon
      cassandraDaemon = new CassandraDaemon
      cassandraDaemon.init(null)
      cassThread = new Thread(new Runnable() {
          def run = cassandraDaemon.start
      })
      cassThread.setDaemon(true)
      cassThread.start
      // give 2 second to start the daemon
      Thread.sleep(2000)

      // create keyspace and column family
      // since cass 0.7, loading schema from storage config is deprecated 
      val cluster = new Cluster("localhost", 19170)
      val cfDef = new CfDef(keyspace, cf)
      val keyspaceManager = new KeyspaceManager(cluster)
      val columnFamilyManager = new ColumnFamilyManager(cluster, keyspace)
      val keyspaceDefinition = new KsDef(keyspace, KeyspaceManager.KSDEF_STRATEGY_SIMPLE, new ArrayList[CfDef])
      keyspaceDefinition.addToCf_defs(cfDef)
      keyspaceManager.addKeyspace(keyspaceDefinition)

    } catch {
      case e: Exception => {
        System.out.println("Embedded casandra server start failed" + e.getMessage())
        stop
      }
    }
  }

  def stop: Unit = {
    println("stop and clean up embedded cassandra...")
    try {
      if (cassThread != null) {
        cassandraDaemon.stop
        cassandraDaemon.destroy
        cassThread.interrupt
        cassThread = null
      }
    } catch {
      case e: Exception => { }
    }
    cleanup
  }

  def cleanup: Unit = {
    try {
      FileUtils.deleteDirectory(new File(baseDir))
      FileUtils.deleteQuietly(new File(homeDir + "/log4j.properties"))
    } catch {
      case e: IOException => {}
    }
  }

}
