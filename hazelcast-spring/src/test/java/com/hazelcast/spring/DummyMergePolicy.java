package com.hazelcast.spring;

import com.hazelcast.core.MapEntry;
import com.hazelcast.merge.MergePolicy;

public class DummyMergePolicy implements MergePolicy {

	public Object merge(String mapName, MapEntry mergingEntry,
			MapEntry existingEntry) {
		return null;
	}

}
