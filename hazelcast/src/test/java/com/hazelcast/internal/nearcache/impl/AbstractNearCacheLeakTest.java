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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.internal.adapter.TransactionalMapDataStructureAdapter;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheInvalidationRequests;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.assertNearCacheSize;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractNearCacheLeakTest<NK, NV> extends HazelcastTestSupport {

    /**
     * The default count to be inserted into the Near Caches.
     */
    protected static final int DEFAULT_RECORD_COUNT = 1000;

    /**
     * The default name used for the data structures which have a Near Cache.
     */
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";

    /**
     * The {@link NearCacheConfig} used by the Near Cache tests.
     * <p>
     * Needs to be set by the implementations of this class in their {@link org.junit.Before} methods.
     */
    protected NearCacheConfig nearCacheConfig;

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K> key type of the created {@link DataStructureAdapter}
     * @param <V> value type of the created {@link DataStructureAdapter}
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext();

    @Test
    public void testNearCacheMemoryLeak() {
        // invalidations have to be enabled, otherwise no RepairHandler is registered
        assertTrue(nearCacheConfig.isInvalidateOnChange());

        NearCacheTestContext<Integer, Integer, NK, NV> context = createContext();

        populateDataAdapter(context, DEFAULT_RECORD_COUNT);
        populateNearCache(context, DEFAULT_RECORD_COUNT);

        assertNearCacheSize(context, DEFAULT_RECORD_COUNT);
        assertNearCacheManager(context, 1);
        assertRepairingTask(context, 1);

        context.nearCacheAdapter.destroy();

        assertNearCacheSize(context, 0);
        assertNearCacheManager(context, 0);
        assertRepairingTask(context, 0);
    }

    protected void populateDataAdapter(NearCacheTestContext<Integer, Integer, ?, ?> context, int size) {
        if (size < 1) {
            return;
        }
        for (int i = 0; i < size; i++) {
            context.dataAdapter.put(i, i);
        }
        if (context.dataAdapter instanceof ReplicatedMapDataStructureAdapter) {
            // FIXME: there are two extra invalidations in the ReplicatedMap
            assertNearCacheInvalidationRequests(context, size + 2);
        } else {
            assertNearCacheInvalidationRequests(context, size);
        }
    }

    protected void populateNearCache(NearCacheTestContext<Integer, Integer, ?, ?> context, int size) {
        for (int i = 0; i < size; i++) {
            context.nearCacheAdapter.get(i);
            context.nearCacheAdapter.get(i);
        }
    }

    protected static void assertNearCacheManager(NearCacheTestContext<Integer, Integer, ?, ?> context, int expected) {
        assertEqualsStringFormat("Expected %d Near Caches in the NearCacheManager, but found %d",
                expected, context.nearCacheManager.listAllNearCaches().size());
    }

    protected static void assertRepairingTask(NearCacheTestContext<Integer, Integer, ?, ?> context, int expected) {
        if (context.nearCacheAdapter instanceof ReplicatedMapDataStructureAdapter) {
            // the ReplicatedMap doesn't support MetaData, so there is are no RepairingHandlers being registered
            return;
        }
        if (context.nearCacheAdapter instanceof TransactionalMapDataStructureAdapter && expected == 0) {
            // FIXME: the TransactionalMapProxy is missing a postDestroy() method to remove its RepairingHandler
            return;
        }
        assertEqualsStringFormat("Expected %d RepairingHandlers in the RepairingTask, but found %d",
                expected, context.repairingTask.getHandlers().size());
    }
}
