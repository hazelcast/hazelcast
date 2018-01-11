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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueueMigrationTest extends HazelcastTestSupport {

    private IQueue<Object> queue;
    private HazelcastInstance remote1;
    private HazelcastInstance remote2;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(3).newInstances();
        HazelcastInstance local = cluster[0];
        remote1 = cluster[1];
        remote2 = cluster[2];

        String name = randomNameOwnedBy(remote1);
        queue = local.getQueue(name);
    }

    @Test
    public void test() {
        List<Object> expectedItems = new LinkedList<Object>();
        for (int i = 0; i < 100; i++) {
            queue.add(i);
            expectedItems.add(i);
        }

        remote1.shutdown();
        remote2.shutdown();

        assertEquals(expectedItems.size(), queue.size());
        List actualItems = Arrays.asList(queue.toArray());
        assertEquals(expectedItems, actualItems);
    }
}
