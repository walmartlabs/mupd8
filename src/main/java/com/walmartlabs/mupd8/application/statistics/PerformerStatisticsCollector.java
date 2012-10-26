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
package com.walmartlabs.mupd8.application.statistics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PerformerStatisticsCollector extends AbstractStatisticsCollector
		implements PrePerformer, PerformerStatisticsCollectorMXBean {

	private long totalEvents = 0;
	private int hotnessThreshold;

	private Map<String, Integer> keyCounts = new HashMap<String, Integer>();
	private Set<String> hotKeys = new HashSet<String>();

	public PerformerStatisticsCollector(String name) {
		super("walmartlabs.com:" + "name" + "=" + name);
	}

	@Override
	public int getKeyCount(String key) {
		if (keyCounts.get(key) != null) {
			return keyCounts.get(key);
		} else {
			return 0;
		}
	}

	public void prePerform(byte[] key, byte[] event) { 
		if (!isEnabled) {
			return;
		}
		totalEvents++;
		String keyValue = new String(key);
		boolean containsKey = keyCounts.containsKey(keyValue);
		synchronized (this) {
			int newCount = containsKey ? keyCounts.get(keyValue) + 1 : 1;
			boolean isHotKey = hotKeys.contains(keyValue);
			if (newCount >= (hotnessThreshold * totalEvents) / 100) {
				if (!isHotKey) {
					hotKeys.add(keyValue);
				}
			} else {
				if (isHotKey) {
					hotKeys.remove(keyValue);
				}
			}
			keyCounts.put(keyValue, newCount);
		}
	}

	@Override
	public String computeCurrentHotKeys() {
		StringBuilder builder = new StringBuilder();
		for (String key : hotKeys) {
			if ((keyCounts.get(key) >= (hotnessThreshold * totalEvents) / 100)) {
				builder.append(key + ":" + keyCounts.get(key));
				builder.append("\n");
			} else {
				hotKeys.remove(key);
			}
		}
		return builder.toString();
	}

	@Override
	public boolean isEnabled() {
		return isEnabled;
	}

	@Override
	public void setEnabled(boolean enable) {
		this.isEnabled = enable;
	}

	@Override
	public Object getManagedBean() {
		return this;
	}

}
