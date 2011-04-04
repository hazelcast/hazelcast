package com.hazelcast.hibernate;

import java.util.Properties;

import org.hibernate.cfg.Environment;

import com.hazelcast.hibernate.provider.HazelcastCacheProvider;

public class CacheProviderDefaultTest extends HibernateStatisticsTestSupport {

	protected Properties getCacheProperties() {
		Properties props = new Properties();
		props.setProperty(Environment.CACHE_PROVIDER, HazelcastCacheProvider.class.getName());
		return props;
	}

}
