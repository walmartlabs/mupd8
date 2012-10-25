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

public class StatisticsBootstrap {

	private boolean initialized = false;
	private String[] statisticsBeans = { "com.walmartlabs.mupd8.application.statistics.NodeStatisticsCollectorFactory" };

	public static final StatisticsBootstrap INSTANCE = new StatisticsBootstrap();

	private StatisticsBootstrap() {
	}

	public void bootstrap() {
		try {
			if (!initialized) {
				for (String beanName : statisticsBeans) {
					BeanManager.INSTANCE
							.registerBean((StatisticsMXBean) ((IStatisticsCollectorFactory) Class
									.forName(beanName).newInstance())
									.getInstance());
				}
				  
				initialized = true;
			} else {
				System.out.println("Already Initialized");
			}
		} catch (Exception e) {
			System.out.println(" Exception in initializing system beans"
					+ e.getMessage());
			System.out
					.println(" System may not have complete statistics reporting");
			e.printStackTrace();
			// we do not raise the exception
			// as the exception here is non-fatal to the other working parts of
			// Muppet
		}
	}
}
