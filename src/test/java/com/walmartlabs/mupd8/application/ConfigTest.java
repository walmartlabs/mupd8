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

package com.walmartlabs.mupd8.application;

import java.io.File;

import junit.framework.TestCase;

import org.json.simple.*;
import com.walmartlabs.mupd8.application.Config;

public class ConfigTest extends TestCase {

    public ConfigTest( String name) {
        super( name);
    }

	@SuppressWarnings("unchecked")
    public void testConfigDirLoad() throws Exception {
        String dir =
            this.getClass().getClassLoader().getResource( "testapp").getPath();
        Config config = new Config( new File( dir));
        String[] paths = { "mupd8", "application", "TestApp", "performers", "K1Updater", "class"};
        String k1Updater = ( String) config.getScopedValue( paths);
        assertEquals("check performer class value", "com.walmartlabs.mupd8.examples.KnUpdaterJson", k1Updater);
        String[] cassPath = { "mupd8", "slate_store", "keyspace"};
        assertEquals("check slate_store" , "Mupd8", ( String) config.getScopedValue( cassPath));

        JSONObject performerConfig = config.workerJSONs.get("K1Updater");
        assertEquals("workerJSONs defined in directory configuration", k1Updater, (String) performerConfig.get("class"));

        String sys =
            this.getClass().getClassLoader().getResource( "testapp/sys_old").getPath();
        String app =
            this.getClass().getClassLoader().getResource( "testapp/app_old").getPath();

        Config newConfig = new Config(sys, app);
        String[] clPath = { "mupd8", "application" };
        java.util.HashMap<String, Object> testApp = ( java.util.HashMap<String, Object>) newConfig.getScopedValue( clPath);
        String firstKey = ( String) testApp.keySet().toArray()[0];
        assertEquals("contains TestApp", "TestApp", firstKey);

        performerConfig = newConfig.workerJSONs.get("K1Updater");
        assertEquals("workerJSONs defined in sys/app configuration", "com.walmartlabs.mupd8.examples.KnUpdater", (String) performerConfig.get("class"));
    }
}
