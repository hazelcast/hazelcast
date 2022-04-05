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

package com.hazelcast.cache.impl;

import com.hazelcast.spi.impl.tenantcontrol.TenantContextual;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.integration.CacheLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        TenantContextual<CacheLoader<String, String>> contextual = TenantContextual.create(() -> delegate,
                () -> true, TenantControl.NOOP_TENANT_CONTROL);
        cacheLoader = new LatencyTrackingCacheLoader<String, String>(contextual, plugin, NAME);
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
