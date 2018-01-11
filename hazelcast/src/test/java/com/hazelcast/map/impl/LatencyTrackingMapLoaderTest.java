/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.MapLoader;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
public class LatencyTrackingMapLoaderTest extends HazelcastTestSupport {

    private static final String NAME = "somemap";

    private HazelcastInstance hz;
    private StoreLatencyPlugin plugin;
    private MapLoader<String, String> delegate;
    private LatencyTrackingMapLoader<String, String> cacheLoader;

    @Before
    public void setup() {
        hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(MapLoader.class);
        cacheLoader = new LatencyTrackingMapLoader<String, String>(delegate, plugin, NAME);
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

    @Test
    public void loadAllKeys() {
        Collection<String> keys = asList("key1", "key2");

        when(delegate.loadAllKeys()).thenReturn(keys);

        Iterable<String> result = cacheLoader.loadAllKeys();

        assertSame(keys, result);
        assertProbeCalledOnce("loadAllKeys");
    }

    public void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingMapLoader.KEY, NAME, methodName));
    }
}
