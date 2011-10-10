package com.hazelcast.impl;

import java.util.Collection;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.impl.FactoryImpl.HazelcastInstanceProxy;
import com.hazelcast.impl.FactoryImpl.ProxyKey;

public interface IHazelcastFactory extends HazelcastInstance {

	HazelcastInstanceProxy getHazelcastInstanceProxy();

	Set<String> getLongInstanceNames();

	Collection<HazelcastInstanceAwareInstance> getProxies();

	Object getOrCreateProxyByName(final String name);

	Object getOrCreateProxy(final ProxyKey proxyKey);
	
	void destroyProxy(final ProxyKey proxyKey);

}