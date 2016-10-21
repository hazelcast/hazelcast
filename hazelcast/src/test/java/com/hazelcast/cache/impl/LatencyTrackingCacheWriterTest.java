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

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import java.util.Collection;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LatencyTrackingCacheWriterTest extends HazelcastTestSupport {

    private static final String NAME = "somecache";

    private HazelcastInstance hz;
    private StoreLatencyPlugin plugin;
    private CacheWriter delegate;
    private LatencyTrackingCacheWriter cacheWriter;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(CacheWriter.class);
        cacheWriter = new LatencyTrackingCacheWriter(delegate, plugin, NAME);
    }

    @Test
    public void write() {
        Cache.Entry entry = new CacheEntry(1, "peter");
        cacheWriter.write(entry);

        verify(delegate).write(entry);
        assertProbeCalledOnce("write");
    }

    @Test
    public void writeAll() {
        Collection c = new LinkedList();

        cacheWriter.writeAll(c);

        verify(delegate).writeAll(c);
        assertProbeCalledOnce("writeAll");
    }

    public void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingCacheWriter.KEY, NAME, methodName));
    }
}
