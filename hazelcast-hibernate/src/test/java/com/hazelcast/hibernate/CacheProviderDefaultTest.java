package com.hazelcast.hibernate;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.hibernate.provider.HazelcastCacheProvider;
import com.hazelcast.impl.GroupProperties;
import org.hibernate.cfg.Environment;
import org.junit.BeforeClass;

import java.util.Properties;

public class CacheProviderDefaultTest extends HibernateStatisticsTestSupport {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        Hazelcast.shutdownAll();
    }

    protected Properties getCacheProperties() {
        Properties props = new Properties();
        props.setProperty(Environment.CACHE_PROVIDER, HazelcastCacheProvider.class.getName());
        return props;
    }
}
