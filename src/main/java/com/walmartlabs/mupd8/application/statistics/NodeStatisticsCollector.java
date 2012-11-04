package com.walmartlabs.mupd8.application.statistics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NodeStatisticsCollector extends AbstractStatisticsCollector
		implements NodeStatisticsCollectorMXBean {

	public static final NodeStatisticsCollector INSTANCE = new NodeStatisticsCollector();

	private static final OperatingSystemMXBean opBean = ManagementFactory
			.getOperatingSystemMXBean();

	private static final MemoryMXBean memBean = ManagementFactory
			.getMemoryMXBean();

	private NodeStatisticsCollector() {
		super("walmartlabs.com:" + "name" + "=" + " node_statistics" + " for "
				+ getHostIPAddress());
	}

	@Override
	public String getNodeLoadAvg() {
		return "" + opBean.getSystemLoadAverage();
	}

	@Override
	public String getNodeHeapMemoryUsage() {
		return "" + memBean.getHeapMemoryUsage();
	}

	private static String getHostIPAddress() {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			return addr.getHostName() + "_" + addr.getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println(" Exception in obtaining hostname/IpAddress!");
			return null;
		}
	}
}
