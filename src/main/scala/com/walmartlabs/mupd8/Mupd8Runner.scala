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

import joptsimple._
import Misc._
import scala.collection.JavaConversions

object Mupd8Runner {
  def main(args : Array[String]) {
    val parser = new OptionParser
    val folderOpt  = parser.accepts("d", "REQUIRED: config file folder containing sys and app configs")
                           .withRequiredArg().ofType(classOf[String])
    val sysOpt     = parser.accepts("s", "DEPRECATED: sys config file")
                           .withRequiredArg().ofType(classOf[String])
    val appOpt     = parser.accepts("a", "DEPRECATED: app config file")
                           .withRequiredArg().ofType(classOf[String])
    val threadsOpt = parser.accepts("threads", "Optonal: number of threads [default 5]")
                           .withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(5)
    val scOpt      = parser.accepts("sc", "Optional: source for sourcereader")
                           .withRequiredArg().ofType(classOf[String])
    val toOpt      = parser.accepts("to", "Optional: destination for sourcereader")
                           .withRequiredArg().ofType(classOf[String])
    val spOpt      = parser.accepts("sp", "Optional: source event key name")
                           .withRequiredArg().ofType(classOf[String])
    val pidOpt     = parser.accepts("pidFile", "mupd8 process PID file")
                           .withRequiredArg().ofType(classOf[String]).defaultsTo("mupd8.pid")

    val options = parser.parse(args : _*)

    // configuration parameters are required
    if (options.has(folderOpt)) {
      System.out.println(folderOpt + " is provided")
    }
    else if (options.has(sysOpt) && options.has(appOpt)) {
      System.out.println(sysOpt + " and " + appOpt + " are provided")
    }
    else {
      System.err.println("Missing arguments: Please provide either " + 
                         folderOpt + " (" + folderOpt.description + ") or " + 
                         sysOpt + " (" + sysOpt.description + ") and " + 
                         appOpt + " (" + appOpt.description + ")")
      System.exit(1)
    }
    /* XXX: loadClasses is always true in Mupd8Runner; TODO: get rid of the parameter */
    var collectStatistics = false
    val appInfo = new AppStaticInfo(if (options.has(folderOpt)) Option(options.valueOf(folderOpt)) else None,
                                    if (options.has(appOpt)) Option(options.valueOf(appOpt)) else None,
                                    if (options.has(sysOpt)) Option(options.valueOf(sysOpt)) else None,
                                    true, collectStatistics)
    // source(s) are required
    if (appInfo.sources.size == 0 &&
        !(options.has(scOpt) && options.has(toOpt) && options.has(spOpt))) {
      System.err.println("\"sources\" is not specified in sys config. " + 
                         "Please either provide sources in sys.cfg or provide arguments " + 
                         scOpt + " (" + scOpt.description + ") " + 
                         toOpt + " (" + toOpt.description + ") and " + 
                         spOpt + " (" + spOpt.description + ")")
      System.exit(1)
    }
    
    if (options.has(pidOpt)) writePID(options.valueOf(pidOpt))
    
    val app = new AppRuntime(0, options.valueOf(threadsOpt), appInfo)
    
    // start the source
    object O {
      def unapply(a: Any): Option[org.json.simple.JSONObject] = 
        if (a.isInstanceOf[org.json.simple.JSONObject]) 
          Some(a.asInstanceOf[org.json.simple.JSONObject])
        else None
    }
    if (appInfo.sources.size > 0) {
      val ssources = JavaConversions.asScalaBuffer(appInfo.sources)
      System.out.println("start source from sys cfg")
			ssources.foreach {
        case O(obj) => {
          if (isLocalHost(obj.get("host").asInstanceOf[String])) {
            val params = obj.get("parameters").asInstanceOf[java.util.List[String]]
            app.startSource(obj.get("performer").asInstanceOf[String], obj.get("source").asInstanceOf[String], params)
          }
        }
        case _ => {println("Wrong source format")}
      }
    }
    else {
      System.out.println("start source from cmdLine")
      app.startSource(options.valueOf(toOpt),
                      options.valueOf(scOpt),
                      JavaConversions.seqAsJavaList(options.valueOf(spOpt).split(',')))
    }
  }
}
