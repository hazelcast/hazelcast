package com.hazelcast.cache.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.integration.CacheLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LatencyTrackingCacheLoaderTest extends HazelcastTestSupport {

    private static final String NAME = "someCache";

    private StoreLatencyPlugin plugin;
    private CacheLoader<String, String> delegate;
    private LatencyTrackingCacheLoader<String, String> cacheLoader;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(CacheLoader.class);
        cacheLoader = new LatencyTrackingCacheLoader<String, String>(delegate, plugin, NAME);
    }

    @Test
    public void load() {
        String key = "key";
        String value = "value";

        when(delegate.load(key)).thenReturn(value);

        String result = cacheLoader.load(key);

        assertSame(value, result);
        assertProbeCalledOnce("load");
    }

    @Test
    public void loadAll() {
        Collection<String> keys = asList("key1", "key2");
        Map<String, String> values = new HashMap<String, String>();
        values.put("key1", "value1");
        values.put("key2", "value2");

        when(delegate.loadAll(keys)).thenReturn(values);

        Map<String, String> result = cacheLoader.loadAll(keys);

        assertSame(values, result);
        assertProbeCalledOnce("loadAll");
    }

    public void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingCacheLoader.KEY, NAME, methodName));
    }
}
