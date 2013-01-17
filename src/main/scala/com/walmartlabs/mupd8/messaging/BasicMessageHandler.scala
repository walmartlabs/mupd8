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
package com.walmartlabs.mupd8.messaging

import com.walmartlabs.mupd8.AppStaticInfo
import com.walmartlabs.mupd8.HashRing
import com.walmartlabs.mupd8.Misc._

class BasicMessageHandler(val staticInfo: AppStaticInfo) extends MessageHandler {

  var ring:HashRing=null

  override def initialize(ring:HashRing) = {
    this.ring=ring
  }


  def actOnMessage(msg: Message) {

    def handleFailedNodeMessage(msg: NodeFailureMessage): Unit = {
      val failedHost = msg.getFailedNodeName()
      val failedHostIpAddress = getIPAddress(failedHost)
      var hostIndex = -1
      val indexed = staticInfo.systemHosts.zipWithIndex
      for(host <- indexed){
         var ipAddress = getIPAddress(host._1)
         if(ipAddress == failedHostIpAddress){
           hostIndex=host._2 
         }
      }
      if(hostIndex != -1){
        if(ring != null){
          ring.remove(hostIndex)
        }
        staticInfo.removeHost(hostIndex)
      } 
      
    }

    def handleNodeJoinMessage(msg: NodeJoinMessage): Unit = {
      System.out.println(" Welcome :" + msg.getJoiningNode())
    }

    def handleHostListMessage(msg: HostListMessage): Unit = {
      System.out.println(" Received host list from message server");
      staticInfo.setSystemHosts(msg.getHostList())
    }

      msg match {
      case msg: NodeFailureMessage => handleFailedNodeMessage(msg)
      case msg: HostListMessage => handleHostListMessage(msg)
      case _ => throw new IllegalStateException(" Unknown message type :" + msg)
    }

    def getStaticInfo() = staticInfo
  }
}
