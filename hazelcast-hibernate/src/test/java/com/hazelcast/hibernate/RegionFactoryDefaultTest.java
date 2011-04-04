package com.hazelcast.hibernate;

import java.util.Properties;

import org.hibernate.cfg.Environment;

public class RegionFactoryDefaultTest extends HibernateStatisticsTestSupport {

	protected Properties getCacheProperties() {
		Properties props = new Properties();
		props.setProperty(Environment.CACHE_REGION_FACTORY, HazelcastCacheRegionFactory.class.getName());
		return props;
	}

}
