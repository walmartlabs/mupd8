package com.walmartlabs.mupd8.application.statistics;

import com.walmartlabs.mupd8.application.binary.Mapper;
import com.walmartlabs.mupd8.application.binary.Performer;
import com.walmartlabs.mupd8.application.binary.PerformerUtilities;

public class MapWrapper implements Mapper {

	private final Mapper mapper;
	private final PrePerformer prePerformer;

	private static final String WRAPPER_SUFFIX = "_wrapper";

	public MapWrapper(Performer mapper, PrePerformer prePerformer) {
		this.mapper = (Mapper) mapper;
		this.prePerformer = prePerformer;
		try {
			BeanManager.INSTANCE.registerBean((StatisticsMXBean) prePerformer
					.getManagedBean());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	@Override
	public String getName() {
		return mapper.getName() + WRAPPER_SUFFIX;
	}

	@Override
	public void map(PerformerUtilities submitter, String stream, byte[] key,
			byte[] event) {
		prePerformer.prePerform(key, event);
		mapper.map(submitter, stream, key, event);
	}

}
