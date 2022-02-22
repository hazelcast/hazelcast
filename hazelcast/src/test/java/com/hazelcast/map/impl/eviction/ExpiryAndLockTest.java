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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpiryAndLockTest extends HazelcastTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void locking_does_not_cause_expired_keys_live_forever() {
        final String KEY = "key";

        final HazelcastInstance node = createHazelcastInstance(getConfig());
        try {
            IMap<String, String> map = node.getMap("test");
            // after 1 second entry should be evicted
            map.set(KEY, "value", 4, TimeUnit.SECONDS);
            // short time after adding it to the map, all ok
            map.lock(KEY);
            Object object = map.get(KEY);
            map.unlock(KEY);
            assertNotNull(object);

            sleepAtLeastSeconds(5);

            // more than one second after adding it, now it should be away
            map.lock(KEY);
            object = map.get(KEY);
            map.unlock(KEY);
            assertNull(object);
        } finally {
            node.shutdown();
        }
    }

    @Test
    public void locked_keys_are_reachable_over_index_after_they_expired() {
        Config config = getConfig();
        config.getMapConfig("default")
                .setTimeToLiveSeconds(2)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> indexedMap = node.getMap("indexed");

        int keyCount = 1_000;
        for (int i = 0; i < keyCount; i++) {
            indexedMap.set(i, i);
            indexedMap.lock(i);
        }

        assertTrueAllTheTime(() -> {
            Set<Map.Entry<Integer, Integer>> entries = indexedMap.entrySet(Predicates.sql("this >= 0"));
            int entrySetSize = CollectionUtil.isEmpty(entries) ? 0 : entries.size();
            assertEquals(keyCount, entrySetSize);
        }, 5);
    }
}
