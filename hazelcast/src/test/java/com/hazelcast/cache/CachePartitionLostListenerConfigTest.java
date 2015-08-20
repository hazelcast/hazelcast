package com.hazelcast.cache;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.cache.impl.HazelcastServerCachingProvider.createCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachePartitionLostListenerConfigTest extends HazelcastTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hazelcast-jcache-partition-lost-listener.xml");

    @Test
    public void testCachePartitionLostListener_registeredViaImplementationInConfigObject() {

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final Config config = new Config();
        final String cacheName = "myCache";
        final CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheName);
        final CachePartitionLostListener listener = mock(CachePartitionLostListener.class);
        cacheConfig.addCachePartitionLostListenerConfig(new CachePartitionLostListenerConfig(listener));
        final int backupCount = 0;
        cacheConfig.setBackupCount(backupCount);

        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        final HazelcastServerCachingProvider cachingProvider = createCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        cacheManager.getCache(cacheName);

        final EventService eventService = getNode(instance).getNodeEngine().getEventService();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final Collection<EventRegistration> registrations = eventService.getRegistrations(CacheService.SERVICE_NAME, cacheName);
                assertFalse(registrations.isEmpty());
            }
        });
    }

    @Test
    public void cacheConfigXmlTest() throws IOException {
        final String cacheName = "cacheWithPartitionLostListener";
        Config config = new XmlConfigBuilder(configUrl).build();
        CacheSimpleConfig cacheConfig = config.getCacheConfig(cacheName);
        List<CachePartitionLostListenerConfig> configs = cacheConfig.getPartitionLostListenerConfigs();
        assertEquals(1, configs.size());
        assertEquals("DummyCachePartitionLostListenerImpl", configs.get(0).getClassName());
    }
}
