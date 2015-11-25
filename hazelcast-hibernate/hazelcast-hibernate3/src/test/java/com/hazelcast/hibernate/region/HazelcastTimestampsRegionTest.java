package com.hazelcast.hibernate.region;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.test.HazelcastParallelClassRunner;
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
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HazelcastTimestampsRegionTest {

    private static final String REGION_NAME = "query.test";

    private int timeout = 60;

    private MapConfig mapConfig;
    private Config config;

    private HazelcastInstance instance;
    private RegionCache cache;
    private HazelcastTimestampsRegion<RegionCache> region;

    @Before
    public void setUp() throws Exception {
        mapConfig = mock(MapConfig.class);
        when(mapConfig.getMaxSizeConfig()).thenReturn(new MaxSizeConfig(50, MaxSizeConfig.MaxSizePolicy.PER_NODE));
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

        cache = mock(RegionCache.class);

        region = new HazelcastTimestampsRegion<RegionCache>(instance, REGION_NAME, new Properties(), cache);
    }

    /**
     * Verifies that the region retrieved the MapConfig and set its timeout from the TTL.
     * Also verifies that the nested LocalRegionCache retrieved the configuration,
     * but did not register a listener on any ITopic.
     */
    @Test
    public void testCacheHonorsConfiguration() {
        assertEquals(cache, region.getCache());

        assertEquals(TimeUnit.SECONDS.toMillis(timeout), region.getTimeout());
        verify(instance, atLeastOnce()).getConfig();
        // ensure a topic is not requested
        verify(instance, never()).getTopic(anyString());
        verify(config, atLeastOnce()).findMapConfig(eq(REGION_NAME));
        // should have been retrieved by the region itself
        verify(mapConfig, times(2)).getTimeToLiveSeconds();
    }

    @Test
    public void testEvict() {
        region.evict("evictionKey");
        verify(cache).remove(eq("evictionKey"));
    }

    @Test
    public void testEvict_withException() {
        doThrow(new OperationTimeoutException("expected exception")).when(cache).remove(eq("evictionKey"));
        region.evict("evictionKey");
        verify(cache).remove(eq("evictionKey"));
    }

    @Test
    public void testEvictAll() {
        region.evictAll();
        verify(cache).clear();
    }

    @Test
    public void testEvictAll_withException() {
        doThrow(new OperationTimeoutException("expected exception")).when(cache).clear();
        region.evictAll();
        verify(cache).clear();
    }

    @Test
    public void testGet() {
        doReturn("getValue").when(cache).get(eq("getKey"), anyLong());
        assertEquals("getValue", region.get("getKey"));
    }

    @Test
    public void testGet_withException() {
        doThrow(new OperationTimeoutException("expected exception")).when(cache).get(eq("getKey"), anyLong());
        assertNull(region.get("getKey"));
        verify(cache).get(eq("getKey"), anyLong());
    }

    @Test
    public void testPut() {
        region.put("putKey", "putValue");
        verify(cache).put(eq("putKey"), eq("putValue"), anyLong(), any());
    }

    @Test
    public void testPut_withException() {
        doThrow(new OperationTimeoutException("expected exception"))
                .when(cache).put(eq("putKey"), eq("putValue"), anyLong(), any());
        region.put("putKey", "putValue");
        verify(cache).put(eq("putKey"), eq("putValue"), anyLong(), any());
    }

    @Test
    public void testCache() {
        assertEquals(cache, region.getCache());
    }
}
