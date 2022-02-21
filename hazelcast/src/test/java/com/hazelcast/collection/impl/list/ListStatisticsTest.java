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

package com.hazelcast.collection.impl.list;

import java.util.Arrays;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.impl.AbstractCollectionStatisticsTest;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListStatisticsTest extends AbstractCollectionStatisticsTest {

    private IList<String> list;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        list = instance.getList(randomString());

        localCollectionStats = list.getLocalListStats();
        previousAccessTime = localCollectionStats.getLastAccessTime();
        previousUpdateTime = localCollectionStats.getLastUpdateTime();
    }

    @Test
    public void testLocalListStats() {
        assertNotEqualsStringFormat("Expected the creationTime not to be %d, but was %d", 0L, localCollectionStats.getCreationTime());
        assertEqualsStringFormat("Expected the lastAccessTime to be %d, but was %d", 0L, localCollectionStats.getLastAccessTime());
        assertEqualsStringFormat("Expected the lastUpdateTime to be %d, but was %d", 0L, localCollectionStats.getLastUpdateTime());

        // an add operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        list.add("element1");
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // and addAll operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        list.addAll(Arrays.asList("element2", "element3"));
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a get operation updates the lastAccessTime, but not the lastUpdateTime
        // we double the operation - with the same parameter - and the last access time check
        // so that we can be sure that assertSameLastUpdateTime() sees an unwanted update
        // at least from the first operation
        sleepMillis(10);
        list.get(0);
        assertNewLastAccessTime();
        sleepMillis(10);
        list.get(0);
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a contains operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        list.contains("element1");
        assertNewLastAccessTime();
        sleepMillis(10);
        list.contains("element1");
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // an indexOf operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        list.indexOf("element1");
        assertNewLastAccessTime();
        sleepMillis(10);
        list.indexOf("element1");
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // an isEmpty operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        list.isEmpty();
        assertNewLastAccessTime();
        sleepMillis(10);
        list.isEmpty();
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a remove operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        list.remove("element2");
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a remove operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        list.remove(0);
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a set operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        list.set(0, "element4");
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a size operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        list.size();
        assertNewLastAccessTime();
        sleepMillis(10);
        list.size();
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a subList operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        list.subList(0, 1);
        assertNewLastAccessTime();
        sleepMillis(10);
        list.subList(0, 1);
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a clear operation updates the lastAccessTime and the lastUpdateTime
        sleepMillis(10);
        list.clear();
        assertNewLastAccessTime();
        assertNewLastUpdateTime();
    }

}
