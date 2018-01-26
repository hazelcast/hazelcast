/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListSplitBrainTest extends SplitBrainTestSupport {

    private final String name = randomString();
    private final int initialCount = 100;
    private final int finalCount = initialCount + 50;

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        IList<Object> list = instances[0].getList(name);

        for (int i = 0; i < initialCount; i++) {
            list.add("item" + i);
        }

        waitAllForSafeState(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {

        IList<Object> list1 = firstBrain[0].getList(name);
        for (int i = initialCount; i < finalCount; i++) {
            list1.add("item" + i);
        }

        IList<Object> list2 = secondBrain[0].getList(name);
        for (int i = initialCount; i < finalCount + 10; i++) {
            list2.add("lost-item" + i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        for (HazelcastInstance instance : instances) {
            IList<Object> list = instance.getList(name);
            assertListContents(list);
        }
    }

    private void assertListContents(IList<Object> list) {
        assertEquals(finalCount, list.size());

        for (int i = 0; i < finalCount; i++) {
            assertEquals("item" + i, list.get(i));
        }
    }
}
