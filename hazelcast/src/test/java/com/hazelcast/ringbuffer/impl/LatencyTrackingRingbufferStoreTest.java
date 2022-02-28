/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.ringbuffer.RingbufferStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LatencyTrackingRingbufferStoreTest extends HazelcastTestSupport {

    private static final String NAME = "LatencyTrackingRingbufferStoreTest";
    private static final ObjectNamespace NAMESPACE = RingbufferService.getRingbufferNamespace(NAME);

    private StoreLatencyPlugin plugin;
    private RingbufferStore<String> delegate;
    private LatencyTrackingRingbufferStore<String> ringbufferStore;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(RingbufferStore.class);
        ringbufferStore = new LatencyTrackingRingbufferStore<String>(delegate, plugin,
                NAMESPACE);
    }

    @Test
    public void load() {
        long sequence = 1L;
        String value = "someValue";

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
        assertEquals(1, plugin.count(LatencyTrackingRingbufferStore.KEY,
                NAMESPACE.getServiceName() + ":" + NAMESPACE.getObjectName(), methodName));
    }
}
