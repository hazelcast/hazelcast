package com.hazelcast.cache;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheQuorumConfigTest extends HazelcastTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hazelcast-jcache-with-quorum.xml");

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
