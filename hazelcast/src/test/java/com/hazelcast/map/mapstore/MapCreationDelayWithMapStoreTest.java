/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.mapstore;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.map.mapstore.writebehind.TestMapUsingMapStoreBuilder;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapCreationDelayWithMapStoreTest extends HazelcastTestSupport {

    @Test(timeout = 120000)
    public void testMapCreation__notAffectedByUnresponsiveLoader() throws Exception {
        final UnresponsiveLoader unresponsiveLoader = new UnresponsiveLoader<Integer, Integer>();
        final IMap map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(unresponsiveLoader)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withPartitionCount(1)
                .build();

        final LocalMapStats stats = map.getLocalMapStats();
        final long ownedEntryCount = stats.getOwnedEntryCount();

        assertEquals(0, ownedEntryCount);
    }


    static class UnresponsiveLoader<K, V> extends MapStoreAdapter<K, V> {

        private final CountDownLatch unreleasedLatch = new CountDownLatch(1);

        @Override
        public Set<K> loadAllKeys() {
            try {
                unreleasedLatch.await();
            } catch (InterruptedException e) {
                //ignore.
            }
            return Collections.emptySet();
        }

    }
}
