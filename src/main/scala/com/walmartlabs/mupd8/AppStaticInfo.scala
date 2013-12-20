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
import java.net.InetAddress
import java.util.ArrayList
import scala.collection.immutable
import scala.collection.breakOut
import scala.collection.JavaConverters._
import grizzled.slf4j.Logging
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.Mupd8Type._

class AppStaticInfo(val configDir: Option[String], val appConfig: Option[String], val sysConfig: Option[String]) extends Logging {
  assert(appConfig.size == sysConfig.size && appConfig.size != configDir.size)

  val config = configDir map { p => new application.Config(new File(p)) } getOrElse new application.Config(sysConfig.get, appConfig.get)
  val performers = loadConfig.convertPerformers(config.workerJSONs)
  debug("performers = " + performers)
  val statusPort = Option(config.getScopedValue(Array("mupd8", "mupd8_status", "http_port"))).getOrElse(new java.lang.Integer(6001)).asInstanceOf[Number].intValue()
  val performerName2ID = Map(performers.map(_.name).zip(0 until performers.size): _*)
  debug("performerName2ID = " + performerName2ID)
  val edgeName2IDs = performers.map(p => p.subs.map((_, performerName2ID(p.name)))).flatten.groupBy(_._1).mapValues(_.map(_._2))
  debug("edgeName2IDs = " + edgeName2IDs)
  var performerArray: Array[binary.Performer] = new Array[binary.Performer](performers.size)

  val performerFactory: Vector[Option[() => binary.Performer]] =
    (0 until performers.size map { i =>
      val performerConfig = performers(i)
      var isMapper = false
      // the wrapper class that wraps a performer instance
      var wrapperClass: Option[String] = null
      info("Loading ... " + performerConfig)
      val constructor: Option[() => binary.Performer] = performerConfig.mtype match {
        case Mapper => {
          isMapper = true; wrapperClass = performerConfig.wrapperClass;
          performerConfig.jclass.map(Class.forName(_)).map { m =>
            if (classOf[binary.Mapper].isAssignableFrom(m)) {
              val mapperConstructor = m.asSubclass(classOf[binary.Mapper]).getConstructor(config.getClass, "".getClass)
              () => mapperConstructor.newInstance(config, performerConfig.name)
            } else {
              val msg = "Mapper " + performerConfig.name + " uses class " + m.getName() + " that is not assignable to " + classOf[binary.Mapper].getName()
              error(msg)
              throw new ClassCastException(msg)
            }
          }
        }
        case Updater => {
          isMapper = false; wrapperClass = performerConfig.wrapperClass;
          performerConfig.jclass.map(Class.forName(_)).map { u =>
            if (classOf[binary.SlateUpdater].isAssignableFrom(u)) {
              if (performerConfig.slateBuilderClass.isEmpty) {
                val msg = "Updater " + performerConfig.name + " uses a SlateUpdater class but does not specify a corresponding slate_builder."
                error(msg)
                info("An updater with no slate_builder may use a class Updater but not SlateUpdater.");
                throw new ClassCastException("Updater " + performerConfig.name + " does not define slate_builder but class " + u.getName() + " implements SlateUpdater, not Updater.")
              }
              val updaterConstructor = u.asSubclass(classOf[binary.SlateUpdater]).getConstructor(config.getClass, "".getClass)
              () => updaterConstructor.newInstance(config, performerConfig.name)
            } else if (classOf[binary.Updater].isAssignableFrom(u)) {
              val updaterFactory = new UpdaterFactory(u.asSubclass(classOf[binary.Updater]))
              () => updaterFactory.construct(config, performerConfig.name)
            } else {
              val msg = "Updater " + performerConfig.name + " uses class " + u + " that is not assignable to " + classOf[binary.Updater].getName()
              error(msg)
              throw new ClassCastException(msg)
            }
          }
        }
        case _ => None
      }
      constructor.map {
        performerConstructor =>
          val performer = performerConstructor()
          performerArray(i) = performer

          if (performerConfig.copy) {
            () => { debug("Building object " + performerConfig.name); performer }
          } else {
            val obj = performer
            () => obj
          }
      }
    })(breakOut)


  val slateBuilderFactory: Vector[Option[() => binary.SlateBuilder]] =
    (0 until performers.size map { i =>
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

  val cassPort = config.getScopedValue(Array("mupd8", "slate_store", "port")).asInstanceOf[Number].intValue()
  val cassKeySpace = config.getScopedValue(Array("mupd8", "slate_store", "keyspace")).asInstanceOf[String]
  val cassHosts = config.getScopedValue(Array("mupd8", "slate_store", "hosts")).asInstanceOf[ArrayList[String]].asScala.toArray
  val cassColumnFamily = config.getScopedValue(Array("mupd8", "application")).asInstanceOf[java.util.HashMap[String, java.lang.Object]].asScala.toMap.head._1
  val cassWriteInterval = Option(config.getScopedValue(Array("mupd8", "slate_store", "write_interval"))) map { _.asInstanceOf[Number].intValue() } getOrElse 15
  val slateCacheCount = Option(config.getScopedValue(Array("mupd8", "slate_store", "slate_cache_count"))) map { _.asInstanceOf[Number].intValue() } getOrElse 1000
  val compressionCodec = Option(config.getScopedValue(Array("mupd8", "slate_store", "compression"))).getOrElse("gzip").asInstanceOf[String].toLowerCase

  var systemHosts: immutable.Map[String, String] = null // Map[ip -> hostname]
  val javaClassPath = Option(config.getScopedValue(Array("mupd8", "java_class_path"))).getOrElse("share/java/*").asInstanceOf[String]

  val sources = Option(config.getScopedValue(Array("mupd8", "sources"))).map {
    x => x.asInstanceOf[java.util.List[org.json.simple.JSONObject]]
  }.getOrElse(new java.util.ArrayList[org.json.simple.JSONObject]())

  val messageServerHostFromConfig: String = Option(config.getScopedValue(Array("mupd8", "messageserver", "host"))) match {
    case None => error("Message server host setting is wrong, exit..."); System.exit(-1); null
    case Some(str) => str.asInstanceOf[String]
  }
  val messageServerPortFromConfig: Int = Option(config.getScopedValue(Array("mupd8", "messageserver", "port"))) match {
    case None => error("Message server port setting is wrong, exiting...");System.exit(-1); -1
    case Some(str) => str.asInstanceOf[Long].toInt
  }

  val internalPort = statusPort + 100

  val startupTimeout = 15
}
