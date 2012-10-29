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

public class NodeStatisticsReport {

	private final String hostname;
	private final long timestamp;
	private final double avgLoad;
	private final String[] hotKeys;

	public static final String host_name = "hostname";
	public static final String time_stamp = "timestamp";
	public static final String avg_load = "avgload";
	public static final String hot_keys = "hotkeys";

	public NodeStatisticsReport(String hostname, long timestamp,
			double avgLoad, String[] hotKeys) {
		this.hostname = hostname;
		this.timestamp = timestamp;
		this.avgLoad = avgLoad;
		this.hotKeys = hotKeys;
	}

	public String getHostname() {
		return hostname;
	}

	public double getAvgLoad() {
		return avgLoad;
	}

	public String[] getHotKeys() {
		return hotKeys;
	}

	public static String getTimeStamp() {
		return time_stamp;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(host_name + "->" + hostname);
		builder.append(",");
		builder.append(time_stamp + "->" + timestamp);
		builder.append(",");
		builder.append(avg_load + "->" + avgLoad);
		builder.append(",");
		builder.append(hot_keys + "->");
		builder.append("(");
		if (hotKeys != null) {
			for (String hotKey : hotKeys) {
				builder.append(hotKey);
				builder.append(",");
			}
			if (hotKeys.length > 0) {
				builder.deleteCharAt(builder.length() - 1);
			}
		}
		builder.append(")");
		return new String(builder);
	};

	public static NodeStatisticsReport initFromString(String content) {
		String hostname = getProperty(content, host_name);
		String timestamp = getProperty(content, time_stamp);
		String avgLoad = getProperty(content, avg_load);
		String hotKeys = content.substring(content.indexOf(hot_keys + "->")
				+ hot_keys.length() + 2);
		String[] keys = null;
		if (!hotKeys.equals("()")) {
			keys = hotKeys.substring(1, hotKeys.length() - 1).split(",");
		}
		return new NodeStatisticsReport(hostname, Long.parseLong(timestamp),
				Double.parseDouble(avgLoad), keys);
	}

	private static String getProperty(String content, String key) {
		String value = content.substring(
				content.indexOf(key + "->") + key.length() + 2,
				content.indexOf(",", content.indexOf(key + "->")));
		return value;

	}

	public static void main(String args[]) {
		NodeStatisticsReport report = new NodeStatisticsReport("localhost",
				System.currentTimeMillis(), 49.334,
				new String[] { "abc", "def" });
		String rep = report.toString();
		System.out.println(" initial report: " + rep);
		NodeStatisticsReport report2 = initFromString(rep);
		System.out.println(" next report:" + report2.toString());
	}

}
