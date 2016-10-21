package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.RingbufferStore;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LatencyTrackingRingbufferStoreTest extends HazelcastTestSupport {
    private static final String NAME = "somerb";

    private HazelcastInstance hz;
    private StoreLatencyPlugin plugin;
    private RingbufferStore<String> delegate;
    private LatencyTrackingRingbufferStore<String> ringbufferStore;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(RingbufferStore.class);
        ringbufferStore = new LatencyTrackingRingbufferStore<String>(delegate, plugin, NAME);
    }

    @Test
    public void load() {
        long sequence = 1L;
        String value = "somevalue";

        when(delegate.load(sequence)).thenReturn(value);

        String result = ringbufferStore.load(sequence);

        assertEquals(result, value);
        assertProbeCalledOnce("load");
    }

    @Test
    public void getLargestSequence() {
        long largestSequence = 100L;
        when(delegate.getLargestSequence()).thenReturn(largestSequence);

        long result = ringbufferStore.getLargestSequence();

        assertEquals(largestSequence, result);
        assertProbeCalledOnce("getLargestSequence");
    }

    @Test
    public void store() {
        long sequence = 1L;
        String value = "value1";

        ringbufferStore.store(sequence, value);

        verify(delegate).store(sequence, value);
        assertProbeCalledOnce("store");
    }

    @Test
    public void storeAll() {
        String[] items = new String[]{"1", "2"};

        ringbufferStore.storeAll(100, items);

        verify(delegate).storeAll(100, items);
        assertProbeCalledOnce("storeAll");
    }

    public void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingRingbufferStore.KEY, NAME, methodName));
    }
}

