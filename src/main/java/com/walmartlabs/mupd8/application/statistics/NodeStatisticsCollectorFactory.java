package com.walmartlabs.mupd8.application.statistics;

public class NodeStatisticsCollectorFactory implements
		IStatisticsCollectorFactory {

	@Override
	public StatisticsMXBean getInstance() {
		return NodeStatisticsCollector.INSTANCE;
	}

}
