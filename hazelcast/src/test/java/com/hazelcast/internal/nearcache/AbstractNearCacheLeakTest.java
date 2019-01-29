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

package com.hazelcast.internal.nearcache;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.adapter.ReplicatedMapDataStructureAdapter;
import com.hazelcast.internal.adapter.TransactionalMapDataStructureAdapter;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractNearCacheLeakTest<NK, NV> extends HazelcastTestSupport {

    /**
     * The default name used for the data structures which have a Near Cache.
     */
    protected static final String DEFAULT_NEAR_CACHE_NAME = "defaultNearCache";

    /**
     * The partition count to configure in the Hazelcast members.
     */
    protected static final String PARTITION_COUNT = "5";

    protected NearCacheConfig nearCacheConfig;

    /**
     * Creates the {@link NearCacheTestContext} used by the Near Cache tests.
     *
     * @param <K>  key type of the created {@link DataStructureAdapter}
     * @param <V>  value type of the created {@link DataStructureAdapter}
     * @param size determines the size the backing {@link DataStructureAdapter} should be populated with
     * @return a {@link NearCacheTestContext} used by the Near Cache tests
     */
    protected abstract <K, V> NearCacheTestContext<K, V, NK, NV> createContext(int size);

    @Test
    public void testNearCacheMemoryLeak() {
        // invalidations have to be enabled, otherwise no RepairHandler is registered
        assertTrue(nearCacheConfig.isInvalidateOnChange());

        NearCacheTestContext<Integer, Integer, NK, NV> context = createContext(1000);

        populateNearCache(context, 1000);
        assertTrue("The Near Cache should be filled (" + context.stats + ")", context.stats.getOwnedEntryCount() > 0);

        assertNearCacheManager(context, 1);
        assertRepairingTask(context, 1);

        context.nearCacheAdapter.destroy();

        assertNearCacheManager(context, 0);
        assertRepairingTask(context, 0);
    }

    protected void assertNearCacheManager(NearCacheTestContext<Integer, Integer, NK, NV> context, int expected) {
        assertEqualsStringFormat("Expected %d Near Caches in the NearCacheManager, but found %d",
                expected, context.nearCacheManager.listAllNearCaches().size());
    }

    protected void assertRepairingTask(NearCacheTestContext<Integer, Integer, NK, NV> context, int expected) {
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

    protected void populateNearCache(NearCacheTestContext<Integer, Integer, NK, NV> context, int size) {
        for (int i = 0; i < size; i++) {
            context.nearCacheAdapter.get(i);
            context.nearCacheAdapter.get(i);
        }
    }

    @SuppressWarnings("unchecked")
    protected static void populateDataAdapter(DataStructureAdapter<?, ?> dataAdapter, int size) {
        if (size < 1) {
            return;
        }
        DataStructureAdapter<Integer, String> adapter = (DataStructureAdapter<Integer, String>) dataAdapter;
        for (int i = 0; i < size; i++) {
            adapter.put(i, "value-" + i);
        }
    }
}
