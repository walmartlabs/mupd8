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

package com.walmartlabs.mupd8.application.binary;

/** An Updater component of an application.
 * 
 * Applications should implement Mapper and Updater interfaces as needed.
 * @todo TODO Distinguish different states of an updater.
 */
public interface SlateUpdater extends Performer {
    public void update(PerformerUtilities submitter, String stream, byte[] key, byte[] event, Object slate);
    
    public Object getDefaultSlate();
}
