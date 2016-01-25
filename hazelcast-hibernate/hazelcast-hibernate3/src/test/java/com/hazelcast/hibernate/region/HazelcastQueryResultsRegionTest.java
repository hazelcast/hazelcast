package com.hazelcast.hibernate.region;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.local.LocalRegionCache;
import com.hazelcast.hibernate.local.LocalRegionCacheTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HazelcastQueryResultsRegionTest {

    private static final String REGION_NAME = "query.test";

    private int maxSize = 50;
    private int timeout = 60;

    private MapConfig mapConfig;
    private Config config;

    private HazelcastInstance instance;
    private HazelcastQueryResultsRegion region;

    @Before
    public void setUp() throws Exception {
        mapConfig = mock(MapConfig.class);
        when(mapConfig.getMaxSizeConfig()).thenReturn(new MaxSizeConfig(maxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE));
        when(mapConfig.getTimeToLiveSeconds()).thenReturn(timeout);

        config = mock(Config.class);
        when(config.findMapConfig(eq(REGION_NAME))).thenReturn(mapConfig);

        Cluster cluster = mock(Cluster.class);
        when(cluster.getClusterTime()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return System.currentTimeMillis();
            }
        });

        instance = mock(HazelcastInstance.class);
        when(instance.getConfig()).thenReturn(config);
        when(instance.getCluster()).thenReturn(cluster);

        region = new HazelcastQueryResultsRegion(instance, REGION_NAME, new Properties());
    }

    /**
     * Verifies that the region retrieved the MapConfig and set its timeout from the TTL.
     * Also verifies that the nested LocalRegionCache retrieved the configuration,
     * but did not register a listener on any ITopic.
     */
    @Test
    public void testCacheHonorsConfiguration() {
        assertEquals(TimeUnit.SECONDS.toMillis(timeout), region.getTimeout());
        verify(instance, atLeastOnce()).getConfig();
        // ensure a topic is not requested
        verify(instance, never()).getTopic(anyString());
        verify(config, atLeastOnce()).findMapConfig(eq(REGION_NAME));
        // should have been retrieved by the region itself
        verify(mapConfig, times(2)).getTimeToLiveSeconds();

        // load the cache with more entries than the configured max size
        LocalRegionCache regionCache = region.getCache();
        assertNotNull(regionCache);

        int overSized = maxSize * 2;
        for (int i = 0; i < overSized; ++i) {
            regionCache.put(i, i, System.currentTimeMillis(), i);
        }
        assertEquals(overSized, regionCache.size());

        // run cleanup to apply the configured limits (the TTL is not tested here for simplicity and speed of this test)
        LocalRegionCacheTest.runCleanup(regionCache);
        // the default size is 100,000, so if the configuration is ignored no elements will be removed.
        // But if the configuration is applied as expected
        assertTrue(regionCache.size() <= 50);
        verify(mapConfig).getMaxSizeConfig();
        verify(mapConfig, times(3)).getTimeToLiveSeconds(); // Should have been retrieved a second time by the cache
    }

    @Test
    public void testEvict() {
        region.evict("evictionKey");
    }

    @Test
    public void testEvictAll() {
        region.evictAll();
    }

    @Test
    public void testGet() {
        assertNull(region.get("getKey"));
    }

    @Test
    public void testPut() {
        region.put("putKey", "putValue");
        assertEquals("putValue", region.get("putKey"));
    }
}
