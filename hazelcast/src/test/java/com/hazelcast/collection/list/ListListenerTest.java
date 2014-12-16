/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.list;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ListListenerTest extends HazelcastTestSupport {

    @Test
    public void testListener() throws Exception {
        final String name = randomString();
        final int count = 10;
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances();
        ListenerTest listener = new ListenerTest(count);
        getList(instances, name).addItemListener(listener, true);

        for (int i = 0; i < count; i++) {
            getList(instances, name).add("item" + i);
        }
        for (int i = 0; i < count; i++) {
            getList(instances, name).remove("item" + i);
        }
        assertTrue(listener.latchAdd.await(5,TimeUnit.SECONDS));
        assertTrue(listener.latchRemove.await(5, TimeUnit.SECONDS));

    }

    private class ListenerTest implements ItemListener {

        CountDownLatch latchAdd;
        CountDownLatch latchRemove;

        ListenerTest(int count) {
            latchAdd = new CountDownLatch(count);
            latchRemove = new CountDownLatch(count);
        }

        @Override
        public void itemAdded(ItemEvent item) {
            latchAdd.countDown();
        }

        @Override
        public void itemRemoved(ItemEvent item) {
            latchRemove.countDown();
        }
    }

    private IList getList(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getList(name);
    }
}
