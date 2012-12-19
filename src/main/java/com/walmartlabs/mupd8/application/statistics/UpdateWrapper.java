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

import com.walmartlabs.mupd8.application.binary.Performer;
import com.walmartlabs.mupd8.application.binary.PerformerUtilities;
import com.walmartlabs.mupd8.application.binary.Slate;
import com.walmartlabs.mupd8.application.binary.Updater;

public class UpdateWrapper implements Updater {

	private Updater updater;
	private PrePerformer prePerformer;

	private static final String WRAPPER_SUFFIX = "_wrapper";

	public UpdateWrapper(Performer updater, PrePerformer preUpdater)
			throws Exception {
		this.updater = (Updater) updater;
		this.prePerformer = preUpdater;
		try {
			BeanManager.INSTANCE.registerBean((StatisticsMXBean) preUpdater
					.getManagedBean());
		} catch (Exception e) {
			//TODO: throw exception to cause failure during startup 
			System.out.println(e.getMessage());
		}

	}

	@Override
	public String getName() {
		return updater.getName() + WRAPPER_SUFFIX;
	}

	@Override
	public void update(PerformerUtilities submitter, String stream, byte[] key,
			byte[] event, Slate slate) {
		prePerformer.prePerform(key, event);
		updater.update(submitter, stream, key, event, slate);
	}

	public void setUpdater(Updater updater) {
		this.updater = updater;
	}

    @Override
    public Slate toSlate(byte[] bytes) {
        return updater.toSlate(bytes);
    }
    
    @Override
    public Slate getDefaultSlate() {
        return updater.getDefaultSlate();
    }

}
