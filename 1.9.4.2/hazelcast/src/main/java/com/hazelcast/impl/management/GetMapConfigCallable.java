package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Prefix;
import com.hazelcast.impl.CMap;
import com.hazelcast.impl.ConcurrentMapManager;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.Processable;
import com.hazelcast.nio.DataSerializable;

public class GetMapConfigCallable implements Callable<MapConfig>, HazelcastInstanceAware, DataSerializable {
	
	private static final long serialVersionUID = -3496139512682608428L;

	protected transient HazelcastInstance hazelcastInstance;
	protected String mapName;

	public GetMapConfigCallable() {
	}

	public GetMapConfigCallable(String mapName) {
		super();
		this.mapName = mapName;
	}

	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;
	}

	public MapConfig call() throws Exception {
		final AtomicReference<MapConfig> ref = new AtomicReference<MapConfig>();
		getClusterService().enqueueAndWait(new Processable() {
			public void process() {
				final CMap cmap = getCMap();
				MapConfig cfg = cmap.getRuntimeConfig();
				ref.set(cfg);
			}
		}, 5);
		return ref.get();
	}
	
	ConcurrentMapManager getConcurrentMapManager() {
		FactoryImpl factory = (FactoryImpl) hazelcastInstance;
		return factory.node.concurrentMapManager;	
	}
	
	ClusterService getClusterService() {
		FactoryImpl factory = (FactoryImpl) hazelcastInstance;
		return factory.node.clusterService;
	}
	
	CMap getCMap() {
		return getConcurrentMapManager().getOrCreateMap(Prefix.MAP + mapName);
	}

	public void writeData(DataOutput out) throws IOException {
		out.writeUTF(mapName);
	}

	public void readData(DataInput in) throws IOException {
		mapName = in.readUTF();
	}
}
