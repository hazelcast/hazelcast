package com.hazelcast.spring;

import java.util.Properties;

import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;

public class DummyStoreFactory implements MapStoreFactory {

	public MapLoader newMapStore(String mapName, Properties properties) {
		return new DummyStore();
	}

}
