/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpirationManagerTimeoutTest extends HazelcastTestSupport {

    @Test
    public void afterShortExpirationEntryShouldBeAway() throws InterruptedException {
        final String KEY = "key";

        final HazelcastInstance node = createHazelcastInstance();
        try {
            IMap<String, String> map = node.getMap("test");
            // after 1 second entry should be evicted
            map.set(KEY, "value", 1500, TimeUnit.MILLISECONDS);
            // short time after adding it to the map, all ok
            map.lock(KEY);
            Object object = map.get(KEY);
            map.unlock(KEY);
            assertNotNull(object);

            sleepAtLeastMillis(1700);

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
    public void afterLongerExpirationEntryShouldBeAway() throws InterruptedException {
        final String KEY = "key";

        final HazelcastInstance node = createHazelcastInstance();
        try {
            IMap<String, String> map = node.getMap("test");
            // after 3 second entry should be evicted
            map.set(KEY, "value", 3, TimeUnit.SECONDS);
            // short time after adding it to the map, all ok
            map.lock(KEY);
            Object object = map.get(KEY);
            map.unlock(KEY);
            assertNotNull(object);

            sleepAtLeastMillis(3600);

            // More than 3 seconds after adding it, now it should be away
            map.lock(KEY);
            object = map.get(KEY);
            map.unlock(KEY);
            assertNull(object);
        } finally {
            node.shutdown();
        }
    }
}
