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

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import java.util.Collection;
import java.util.LinkedList;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LatencyTrackingCacheWriterTest extends HazelcastTestSupport {

    private static final String NAME = "someCache";

    private StoreLatencyPlugin plugin;
    private CacheWriter<Integer, String> delegate;
    private LatencyTrackingCacheWriter<Integer, String> cacheWriter;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        plugin = new StoreLatencyPlugin(getNodeEngineImpl(hz));
        delegate = mock(CacheWriter.class);
        TenantContextual<CacheWriter<Integer, String>> contextual = TenantContextual.create(() -> delegate,
                () -> true, TenantControl.NOOP_TENANT_CONTROL);
        cacheWriter = new LatencyTrackingCacheWriter<Integer, String>(contextual, plugin, NAME);
    }

    @Test
    public void write() {
        Cache.Entry<Integer, String> entry = new CacheEntry<Integer, String>(1, "peter");
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

    @Test
    public void delete() {
        Cache.Entry<Integer, String> entry = new CacheEntry<Integer, String>(1, "peter");
        cacheWriter.delete(entry);

        verify(delegate).delete(entry);
        assertProbeCalledOnce("delete");
    }

    @Test
    public void deleteAll() {
        Collection c = new LinkedList();

        cacheWriter.deleteAll(c);

        verify(delegate).deleteAll(c);
        assertProbeCalledOnce("deleteAll");
    }

    private void assertProbeCalledOnce(String methodName) {
        assertEquals(1, plugin.count(LatencyTrackingCacheWriter.KEY, NAME, methodName));
    }
}
