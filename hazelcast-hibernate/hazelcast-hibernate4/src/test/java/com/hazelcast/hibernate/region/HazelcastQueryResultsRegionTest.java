package com.hazelcast.hibernate.region;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.local.LocalRegionCache;
import com.hazelcast.hibernate.local.LocalRegionCacheTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HazelcastQueryResultsRegionTest {

    private static final String REGION_NAME = "query.test";

    @Test
    public void testCacheHonorsConfiguration() {
        int maxSize = 50;
        int timeout = 60;

        MapConfig mapConfig = mock(MapConfig.class);
        when(mapConfig.getMaxSizeConfig()).thenReturn(new MaxSizeConfig(maxSize, MaxSizeConfig.MaxSizePolicy.PER_NODE));
        when(mapConfig.getTimeToLiveSeconds()).thenReturn(timeout);

        Config config = mock(Config.class);
        when(config.findMapConfig(eq(REGION_NAME))).thenReturn(mapConfig);

        Cluster cluster = mock(Cluster.class);
        when(cluster.getClusterTime()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return System.currentTimeMillis();
            }
        });

        HazelcastInstance instance = mock(HazelcastInstance.class);
        when(instance.getConfig()).thenReturn(config);
        when(instance.getCluster()).thenReturn(cluster);

        // Create the region and verify that it retrieved the MapConfig and set its timeout from
        // the TTL. Also verify that the nested LocalRegionCache retrieved the configuration but
        // did _not_ register a listener on any ITopic
        HazelcastQueryResultsRegion region = new HazelcastQueryResultsRegion(instance, REGION_NAME, new Properties());
        assertEquals(TimeUnit.SECONDS.toMillis(timeout), region.getTimeout());
        verify(instance, atLeastOnce()).getConfig();
        verify(instance, never()).getTopic(anyString()); // Ensure a topic is not requested
        verify(config, atLeastOnce()).findMapConfig(eq(REGION_NAME));
        verify(mapConfig, times(2)).getTimeToLiveSeconds(); // Should have been retrieved by the region itself

        // Next, load the cache with more entries than the configured max size
        LocalRegionCache regionCache = region.getCache();
        assertNotNull(regionCache);

        int oversized = maxSize * 2;
        for (int i = 0; i < oversized; ++i) {
            regionCache.put(i, i, System.currentTimeMillis(), i);
        }
        assertEquals(oversized, regionCache.size());

        // Lastly run cleanup to apply the configured limits. Note that the TTL is not tested here
        // simply for simplicity (and for the speed of this test)
        LocalRegionCacheTest.runCleanup(regionCache);
        // The default size is 100,000, so if the configuration is ignored no elements will be removed. But
        // if the configuration is applied as expected
        assertTrue(regionCache.size() <= 50);
        verify(mapConfig).getMaxSizeConfig();
        verify(mapConfig, times(3)).getTimeToLiveSeconds(); // Should have been retrieved a second time by the cache
    }
}