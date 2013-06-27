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
import java.util.ArrayList
import scala.collection.breakOut
import scala.collection.JavaConverters._
import grizzled.slf4j.Logging
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.Mupd8Type._
import com.walmartlabs.mupd8.application.statistics.StatisticsConstants
import com.walmartlabs.mupd8.application.statistics.MapWrapper
import com.walmartlabs.mupd8.application.statistics.UpdateWrapper
import java.net.InetAddress

class AppStaticInfo(val configDir: Option[String], val appConfig: Option[String], val sysConfig: Option[String], val loadClasses: Boolean, statistics: Boolean, elastic: Boolean) extends Logging {
  assert(appConfig.size == sysConfig.size && appConfig.size != configDir.size)

  val config = configDir map { p => new application.Config(new File(p)) } getOrElse new application.Config(sysConfig.get, appConfig.get)
  val performers = loadConfig.convertPerformers(config.workerJSONs)
  val statusPort = Option(config.getScopedValue(Array("mupd8", "mupd8_status", "http_port"))).getOrElse(new java.lang.Integer(6001)).asInstanceOf[Number].intValue()
  val performerName2ID = Map(performers.map(_.name).zip(0 until performers.size): _*)
  debug("performerName2ID = " + performerName2ID)
  val edgeName2IDs = performers.map(p => p.subs.map((_, performerName2ID(p.name)))).flatten.groupBy(_._1).mapValues(_.map(_._2))
  debug("edgeName2IDs = " + edgeName2IDs)
  var performerArray: Array[binary.Performer] = new Array[binary.Performer](performers.size)

  val performerFactory: Vector[Option[() => binary.Performer]] = if (loadClasses) (
    0 until performers.size map { i =>
      val p = performers(i)
      var isMapper = false
      // the wrapper class that wraps a performer instance
      var wrapperClass: Option[String] = null
      info("Loading ... " + p.name + " " + p.mtype)
      val constructor : Option[() => binary.Performer] = p.mtype match {
        case Mapper => {
          isMapper = true; wrapperClass = p.wrapperClass;
          p.jclass.map(Class.forName(_)).map { m =>
            if (classOf[binary.Mapper].isAssignableFrom(m)) {
              val mapperConstructor = m.asSubclass(classOf[binary.Mapper]).getConstructor(config.getClass, "".getClass)
              () => mapperConstructor.newInstance(config, p.name)
            } else {
              val msg = "Mapper "+p.name+" uses class "+m.getName()+" that is not assignable to "+classOf[binary.Mapper].getName()
              error(msg)
              throw new ClassCastException(msg)
            }
          }
        }
        case Updater => {
          isMapper = false; wrapperClass = p.wrapperClass;
          p.jclass.map(Class.forName(_)).map { u =>
            if (classOf[binary.SlateUpdater].isAssignableFrom(u)) {
              if (p.slateBuilderClass.isEmpty) {
                val msg = "Updater "+p.name+" uses a SlateUpdater class but does not specify a corresponding slate_builder."
                error(msg)
                info("An updater with no slate_builder may use a class Updater but not SlateUpdater.");
                throw new ClassCastException("Updater "+p.name+" does not define slate_builder but class "+u.getName()+" implements SlateUpdater, not Updater.")
              }
              val updaterConstructor = u.asSubclass(classOf[binary.SlateUpdater]).getConstructor(config.getClass, "".getClass)
              () => updaterConstructor.newInstance(config, p.name)
            } else if (classOf[binary.Updater].isAssignableFrom(u)) {
              val updaterFactory = new UpdaterFactory(u.asSubclass(classOf[binary.Updater]))
              () => updaterFactory.construct(config, p.name)
            } else {
              val msg = "Updater "+p.name+" uses class "+u+" that is not assignable to "+classOf[binary.Updater].getName()
              error(msg)
              throw new ClassCastException(msg)
            }
          }
        }
        case _ => None
      }
      constructor.map { performerConstructor =>
        val needToCollectStatistics = statistics | elastic
        val wrappedPerformer =
          if (statistics) {
            val wrapperClassname = wrapperClass match {
              case None => StatisticsConstants.DEFAULT_PRE_PERFORMER
              case Some(x) => x
            }
            var constructor = Class.forName(wrapperClassname).asInstanceOf[Class[com.walmartlabs.mupd8.application.statistics.PrePerformer]].getConstructor("".getClass());
            val prePerformer = constructor.newInstance(p.name)
            if (isMapper) {
              new MapWrapper(performerConstructor().asInstanceOf[binary.Mapper], prePerformer)
            } else {
              new UpdateWrapper(performerConstructor().asInstanceOf[binary.SlateUpdater], prePerformer)
            }
          } else {
            performerConstructor()
          }

        performerArray(i) = wrappedPerformer

        if (p.copy) {
          //        if ((p.name == "fbEntityProcessor")||(p.name == "interestStatsFetcher")) {
          () => { debug("Building object " + p.name); wrappedPerformer }
        } else {
          val obj = wrappedPerformer
          () => obj
        }
      }
    })(breakOut)
  else Vector()

  val slateBuilderFactory: Vector[Option[() => binary.SlateBuilder]] = if (loadClasses) (
    0 until performers.size map { i =>
      val p = performers(i)
      val slateBuilder = p.mtype match {
        case Updater => {
          // TODO Keep enough state to detect the error case: SlateUpdater but no SlateBuilder specified.
          val slateBuilderClass = p.slateBuilderClass.map(Class.forName(_)).getOrElse(classOf[binary.ByteArraySlateBuilder])
          val castSlateBuilderClass = try {
            slateBuilderClass.asSubclass(classOf[binary.SlateBuilder])
          } catch {
            case e : ClassCastException => {
              error("SlateBuilder class " + slateBuilderClass.getName() + " for updater " + p.name + " is not an implementation of " + classOf[binary.SlateBuilder].getName()+" as required: ", e)
              throw e
            }
          }
          val slateBuilderConstructor = castSlateBuilderClass.getConstructor(config.getClass, "".getClass)
          Some(() => { slateBuilderConstructor.newInstance(config, p.name) })
        }
        case _ => None
      }
      slateBuilder
    })(breakOut)
  else Vector()

  val cassPort = config.getScopedValue(Array("mupd8", "slate_store", "port")).asInstanceOf[Number].intValue()
  val cassKeySpace = config.getScopedValue(Array("mupd8", "slate_store", "keyspace")).asInstanceOf[String]
  val cassHosts = config.getScopedValue(Array("mupd8", "slate_store", "hosts")).asInstanceOf[ArrayList[String]].asScala.toArray
  val cassColumnFamily = config.getScopedValue(Array("mupd8", "application")).asInstanceOf[java.util.HashMap[String, java.lang.Object]].asScala.toMap.head._1
  val cassWriteInterval = Option(config.getScopedValue(Array("mupd8", "slate_store", "write_interval"))) map { _.asInstanceOf[Number].intValue() } getOrElse 15
  val slateCacheCount = Option(config.getScopedValue(Array("mupd8", "slate_store", "slate_cache_count"))) map { _.asInstanceOf[Number].intValue() } getOrElse 1000
  val compressionCodec = Option(config.getScopedValue(Array("mupd8", "slate_store", "compression"))).getOrElse("gzip").asInstanceOf[String].toLowerCase

  var systemHosts: scala.collection.immutable.Map[String, String] = null
  val javaClassPath = Option(config.getScopedValue(Array("mupd8", "java_class_path"))).getOrElse("share/java/*").asInstanceOf[String]
  val javaSetting = Option(config.getScopedValue(Array("mupd8", "java_setting"))).getOrElse("-Xmx200M -Xms200M").asInstanceOf[String]

  val sources = Option(config.getScopedValue(Array("mupd8", "sources"))).map {
    x => x.asInstanceOf[java.util.List[org.json.simple.JSONObject]]
  }.getOrElse(new java.util.ArrayList[org.json.simple.JSONObject]())

  val messageServerHost = Option(config.getScopedValue(Array("mupd8", "messageserver", "host")))
  val messageServerPort = Option(config.getScopedValue(Array("mupd8", "messageserver", "port")))

  // Try to detect node's hostname and ipaddress by connecting to message server
  private def getHostName(retryCount: Int): Host = {
    if (retryCount > 10 || messageServerHost == None || messageServerPort == None) {
      Host(InetAddress.getLocalHost.getHostAddress, InetAddress.getLocalHost.getHostName)
    } else {
      try {
    	val s = new java.net.Socket(messageServerHost.get.asInstanceOf[String], messageServerPort.get.asInstanceOf[Long].toInt)
    	val host = Host(s.getLocalAddress.getHostAddress, s.getLocalAddress.getHostName)
    	s.close
    	host
      } catch {
        case e: Exception => warn("getHostName: Connect to message server failed, retry", e); getHostName(retryCount + 1)
      }
    }
  }
  info("Connect to message server " + (messageServerHost, messageServerPort) + " to decide hostname")
  val self: Host = if (messageServerHost == None || messageServerPort == None) 
                     Host(InetAddress.getLocalHost.getHostAddress, InetAddress.getLocalHost.getHostName)
                   else 
                     new MessageServerClient(messageServerHost.get.asInstanceOf[String], messageServerPort.get.asInstanceOf[Long].toInt).checkIP
  
  info("Host id is " + self)

  def internalPort = statusPort + 100;
}
