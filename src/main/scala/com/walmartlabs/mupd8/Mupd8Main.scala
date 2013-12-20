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

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.breakOut
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.actors.Actor
import java.util.ArrayList
import org.scale7.cassandra.pelops.Mutator
import grizzled.slf4j.Logging
import com.walmartlabs.mupd8.application._
import com.walmartlabs.mupd8.Misc._
import com.walmartlabs.mupd8.Mupd8Type._

case class Host(ip: String, hostname: String)

case class Performer(name: String,
  pubs: Vector[String],
  subs: Vector[String],
  mtype: Mupd8Type,
  ptype: Option[String],
  jclass: Option[String],
  wrapperClass: Option[String],
  slateBuilderClass: Option[String],
  workers: Int,
  cf: Option[String],
  ttl: Int,
  copy: Boolean)

object loadConfig {

  def isTrue(value: Option[String]): Boolean = {
    (value != null) && (value != None) && (value.get.toLowerCase != "false") && (value.get.toLowerCase != "off") && (value.get != "0")
  }

  def convertPerformers(pHashMap: java.util.HashMap[String, org.json.simple.JSONObject]) = {
    val performers = pHashMap.asScala.toMap
    def convertStrings(list: java.util.List[String]): Vector[String] = {
      if (list == null) Vector() else list.asScala.toArray.map(p => p)(breakOut)
    }

    performers.map(p =>
      Performer(
        name = p._1,
        pubs = convertStrings(p._2.get("publishes_to").asInstanceOf[ArrayList[String]]),
        subs = convertStrings(p._2.get("subscribes_to").asInstanceOf[ArrayList[String]]),
        mtype = Mupd8Type.withName(p._2.get("mupd8_type").asInstanceOf[String]),
        ptype = Option(p._2.get("type").asInstanceOf[String]),
        jclass = Option(p._2.get("class").asInstanceOf[String]),
        wrapperClass = { Option(p._2.get("wrapper_class").asInstanceOf[String]) },
        slateBuilderClass = Option(p._2.get("slate_builder").asInstanceOf[String]),
        workers = if (p._2.get("workers") == null) 1.toInt else p._2.get("workers").asInstanceOf[Number].intValue(),
        cf = Option(p._2.get("column_family").asInstanceOf[String]),
        ttl = if (p._2.get("slate_ttl") == null) Mutator.NO_TTL else p._2.get("slate_ttl").asInstanceOf[Number].intValue(),
        copy = isTrue(Option(p._2.get("clone").asInstanceOf[String]))))(breakOut)
  }

}

// A factory that constructs a SlateUpdater that runs an Updater.
// The SlateUpdater expects to be accompanied by a ByteArraySlateBuilder as
// its SlateBuilder so that the slate object indeed stays the raw byte[].
class UpdaterFactory[U <: binary.Updater](val updaterType : Class[U]) {
  val updaterConstructor = updaterType.getConstructor(classOf[Config], classOf[String])
  def construct(config : Config, name : String) : binary.SlateUpdater = {
    val updater = updaterConstructor.newInstance(config, name)
    val updaterWrapper = new binary.SlateUpdater() {
      override def getName() = updater.getName()
      override def update(util : binary.PerformerUtilities, stream : String, k : Array[Byte], v : Array[Byte], slate : SlateObject) = {
        updater.update(util, stream, k, v, slate.asInstanceOf[Array[Byte]])
      }
      override def getDefaultSlate() : Array[Byte] = Array[Byte]()
    }
    updaterWrapper
  }
}

object Mupd8Main extends Logging {

  def main(args: Array[String]) {
    info("Mupd8 is starting ...")
    Thread.setDefaultUncaughtExceptionHandler(new Misc.TerminatingExceptionHandler())

    val syntax = Map("-s" -> (1, "Sys config file name"),
      "-a" -> (1, "App config file name"),
      "-d" -> (1, "Unified-config directory name"),
      "-sc" -> (1, "Mupd8 source class name"),
      "-sp" -> (1, "Mupd8 source class parameters separated by comma"),
      "-to" -> (1, "Stream to which data from the URI is sent"),
      "-threads" -> (1, "Optional number of execution threads, default is 5"),
      "-shutdown" -> (0, "Shut down the Mupd8 App"),
      "-pidFile" -> (1, "Optional PID filename"))

    val argMap = argParser(syntax, args)
    if (argMap.get("-s").size != argMap.get("-a").size || argMap.get("-s").size == argMap.get("-d").size) {
      error("Command parameter \"-s, -a, -d\" error, exiting...")
      System.exit(-1)
    }
    val threads = argMap.get("-threads").map(_.head.toInt).getOrElse(5)
    val shutdown = !argMap.get("-shutdown").isDefined
    val appStatic = new AppStaticInfo(argMap.get("-d").map(_.head), argMap.get("-a").map(_.head), argMap.get("-s").map(_.head))
    argMap.get("-pidFile") match {
      case None => writePID("mupd8.log")
      case Some(x) => writePID(x.head)
    }

    val runtime = new AppRuntime(0, threads, appStatic)
    if (runtime.ring != null) {
      if (appStatic.sources.size > 0) {
        startSources(appStatic, runtime)
      } else if (argMap.contains("-to") && argMap.contains("-sc")) {
        info("start source from cmdLine")
        runtime.startSource("cmdLineSource", argMap("-to").head, argMap("-sc").head, seqAsJavaList(argMap("-sp").head.split(',')))
      }
    } else {
      error("Mupd8Main: no hash ring found, exiting...")
    }
    info("Initialization is done")
  }

  def startSources(app: AppStaticInfo, runtime: AppRuntime) {
    class AskPermit(sourceName: String) extends Actor {
      def act() {
        val client: MessageServerClient = new MessageServerClient(runtime, 1000)
        client.sendMessage(AskPermitToStartSourceMessage(sourceName, runtime.self))
      }
    }

    val ssources = app.sources.asScala
    info("start source from sys cfg")
    ssources.foreach { source =>
      if (isLocalHost(source.get("host").asInstanceOf[String])) {
        info("Start source - " + source + " on this node")
        val sourceName = source.get("name").asInstanceOf[String]
        val askPermit = new AskPermit(sourceName)
        askPermit.start
      }
    }
  }

}
