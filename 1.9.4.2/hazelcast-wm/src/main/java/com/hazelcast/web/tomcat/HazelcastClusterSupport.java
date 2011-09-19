package com.hazelcast.web.tomcat;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class HazelcastClusterSupport {

	private static final String SESSION_ATTR_MAP = "__hz_ses_attrs";
	private static final HazelcastClusterSupport INSTANCE = new HazelcastClusterSupport();
	
	public static HazelcastClusterSupport get() {
		return INSTANCE;
	}
	
	private final HazelcastInstance hazelcast;
	private IMap<String, HazelcastAttribute> sessionMap;
	
	private HazelcastClusterSupport() {
		super();
		hazelcast = Hazelcast.newHazelcastInstance(null);
	}
	
	public IMap<String, HazelcastAttribute> getAttributesMap() {
		return sessionMap;
	}
	
	public void start() {
		sessionMap = hazelcast.getMap(SESSION_ATTR_MAP);
	}
	
	public void stop() {
		hazelcast.getLifecycleService().shutdown();
	}
}
