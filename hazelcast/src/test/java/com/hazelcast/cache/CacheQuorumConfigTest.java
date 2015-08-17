package com.hazelcast.cache;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CacheSimpleEntryListenerConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Caching;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CacheQuorumConfigTest extends HazelcastTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hazelcast-jcache-with-quorum.xml");

    @Before
    @After
    public void cleanup() {
        HazelcastInstanceFactory.terminateAll();
        Caching.getCachingProvider().close();
    }

    @Test
    public void cacheConfigXmlTest() throws IOException {
        final String cacheName = "configtestCache" + randomString();
        Config config = new XmlConfigBuilder(configUrl).build();
        CacheSimpleConfig cacheConfig1 = config.getCacheConfig(cacheName);
        final String quorumName = cacheConfig1.getQuorumName();

        assertEquals("cache-quorum", quorumName);

        QuorumConfig quorumConfig = config.getQuorumConfig(quorumName);
        assertEquals(3, quorumConfig.getSize());
        assertEquals(QuorumType.READ_WRITE, quorumConfig.getType());
    }
}
