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

package com.hazelcast.map.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.map.MapStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
