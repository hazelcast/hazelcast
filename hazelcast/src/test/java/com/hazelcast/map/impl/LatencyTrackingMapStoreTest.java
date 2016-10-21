package com.hazelcast.map.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStore;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LatencyTrackingMapStoreTest extends HazelcastTestSupport {

    private static final String NAME = "somemap";

    private HazelcastInstance hz;
    private StoreLatencyPlugin plugin;
    private MapStore<String, String> delegate;
    private LatencyTrackingMapStore<String, String> cacheStore;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(MapStore.class);
        cacheStore = new LatencyTrackingMapStore<String, String>(delegate, plugin, NAME);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void load() {
        cacheStore.load("somekey");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void loadAll() {
        cacheStore.loadAll(asList("1", "2"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void loadAllKeys() {
        cacheStore.loadAllKeys();
    }

    @Test
    public void store() {
        String key = "somekey";
        String value = "somevalue";

        cacheStore.store(key, value);

        verify(delegate).store(key, value);
        assertProbeCalledOnce("store");
    }

    @Test
    public void storeAll() {
        Map<String, String> values = new HashMap<String, String>();
        values.put("1", "value1");
        values.put("2", "value2");

        cacheStore.storeAll(values);

        verify(delegate).storeAll(values);
        assertProbeCalledOnce("storeAll");
    }

    @Test
    public void delete() {
        String key = "somekey";

        cacheStore.delete(key);

        verify(delegate).delete(key);
        assertProbeCalledOnce("delete");
    }

    @Test
    public void deleteAll() {
        List<String> keys = Arrays.asList("1", "2");

        cacheStore.deleteAll(keys);

        verify(delegate).deleteAll(keys);
        assertProbeCalledOnce("deleteAll");
    }

    public void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingMapLoader.KEY, NAME, methodName));
    }
}
