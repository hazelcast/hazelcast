/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.MapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapHazelcastInstanceAwareTest extends HazelcastTestSupport {
    private CompletableFuture<HazelcastInstance> hazelcastInstanceFuture;

    @Before
    public void setUp() {
        hazelcastInstanceFuture = new CompletableFuture<>();
    }

    @Test
    public void testInjected() {
        createHazelcastInstance().getMultiMap(randomMapName()).addEntryListener(new MyEntryListener<>(), false);

        assertTrueEventually(() -> assertNotNull(hazelcastInstanceFuture.get()));
    }

    private class MyEntryListener<K, V> implements EntryListener<K, V>, HazelcastInstanceAware {
        @Override
        public void entryAdded(EntryEvent<K, V> event) {
        }

        @Override
        public void entryUpdated(EntryEvent<K, V> event) {
        }

        @Override
        public void entryRemoved(EntryEvent<K, V> event) {
        }

        @Override
        public void entryEvicted(EntryEvent<K, V> event) {
        }

        @Override
        public void entryExpired(EntryEvent<K, V> event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance != null) {
                hazelcastInstanceFuture.complete(hazelcastInstance);
            }
        }
    }
}
