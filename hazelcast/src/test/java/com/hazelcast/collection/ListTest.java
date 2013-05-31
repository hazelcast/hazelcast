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

package com.hazelcast.collection;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @ali 3/6/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class ListTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void testListMethods() throws Exception {
        Config config = new Config();
        final String name = "defList";
        final int count = 100;
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        for (int i=0; i<count; i++){
            assertTrue(getList(instances, name).add("item"+i));
        }

        Iterator iter = getList(instances, name).iterator();
        int item = 0;
        while (iter.hasNext()){
            assertEquals("item"+item++, iter.next());
        }

        assertEquals(count, getList(instances, name).size());

        assertEquals("item0", getList(instances, name).get(0));
        getList(instances, name).add(0, "item");
        assertEquals(count+1, getList(instances, name).size());
        assertEquals("item",getList(instances, name).set(0, "newItem"));
        assertEquals("newItem",getList(instances, name).remove(0));
        assertEquals(count, getList(instances, name).size());
        assertTrue(getList(instances, name).remove("item99"));
        assertFalse(getList(instances, name).remove("item99"));

        List list = new ArrayList();
        list.add("item-1");
        list.add("item-2");

        assertTrue(getList(instances, name).addAll(list));
        assertEquals("item-1", getList(instances, name).get(count-1));
        assertEquals("item-2", getList(instances, name).get(count));
        assertEquals(count+list.size()-1, getList(instances, name).size());
        assertTrue(getList(instances, name).addAll(10,list));
        assertEquals("item-1", getList(instances, name).get(10));
        assertEquals("item-2", getList(instances, name).get(11));
        assertEquals(count+2*list.size()-1, getList(instances, name).size());
        assertEquals(10, getList(instances, name).indexOf("item-1"));
        assertEquals(count-1+list.size(), getList(instances, name).lastIndexOf("item-1"));

        assertTrue(getList(instances, name).containsAll(list));
        list.add("asd");
        assertFalse(getList(instances, name).containsAll(list));
        assertTrue(getList(instances, name).contains("item98"));
        assertFalse(getList(instances, name).contains("item99"));
    }

    @Test
    public void testListener() throws Exception {
        Config config = new Config();
        final String name = "defList";
        final int count = 10;
        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        final CountDownLatch latchAdd = new CountDownLatch(count);
        final CountDownLatch latchRemove = new CountDownLatch(count);

        ItemListener listener = new ItemListener() {
            public void itemAdded(ItemEvent item) {
                latchAdd.countDown();
            }

            public void itemRemoved(ItemEvent item) {
                latchRemove.countDown();
            }
        };

        getList(instances, name).addItemListener(listener, true);

        for (int i = 0; i < count; i++) {
            getList(instances, name).add("item" + i);
        }
        for (int i = 0; i < count; i++) {
            getList(instances, name).remove("item"+i);
        }
        assertTrue(latchAdd.await(5, TimeUnit.SECONDS));
        assertTrue(latchRemove.await(5, TimeUnit.SECONDS));

    }

    private IList getList(HazelcastInstance[] instances, String name){
        final Random rnd = new Random(System.currentTimeMillis());
        return instances[rnd.nextInt(instances.length)].getList(name);
    }


}
