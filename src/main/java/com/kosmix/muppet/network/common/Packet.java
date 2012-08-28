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

package com.walmartlabs.mupd8.network.common;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

public class Packet implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private int priority;
	private int performerID;
    private byte[] key;
    private byte[] event;
    private byte[] stream;

    public Packet() {
    }

    public Packet(int priority, int performerID, byte[] key, byte[] event, byte[] stream) {
        this.priority = priority;
        this.performerID = performerID;
        this.key = key;
        this.event = event;
        this.stream = stream;
    }

    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
    
    public int getPerformerID() { return performerID; }
    public void setPerformerID(int performerID) { this.performerID = performerID; }

    public byte[] getKey() { return key; }
    public void setKey(byte[] key) { this.key = key; }

    public byte[] getEvent() { return this.event; }
    public void setEvent(byte[] event) { this.event = event; }

    public byte[] getStream() { return this.stream; }
    public void setStream(byte[] stream) { this.stream = stream; }
    
    @Override
    public String toString() {
    	try {
    		return new StringBuilder()
    		.append("{")
    		.append("priority=").append(priority)
    		.append(", performerID=").append(performerID)
    		.append(", key=").append(new String(key, "UTF-8"))
    		.append(", event=").append(new String(event, "UTF-8"))
    		.append(", stream=").append(new String(stream, "UTF-8"))
    		.append("}").toString();
    	} catch (UnsupportedEncodingException e) {
    		System.err.println(e.getMessage());
    		return null;
    	}
    }
}
