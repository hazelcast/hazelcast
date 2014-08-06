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
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author ali 3/6/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ListTest extends HazelcastTestSupport {

    @Test
    @ClientCompatibleTest
    public void testListMethods() throws Exception {
        Config config = new Config();
        final String name = "defList";
        final int count = 100;
        final int insCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        assertTrue(getList(instances, name).isEmpty());
        for (int i=0; i<count; i++){
            assertTrue(getList(instances, name).add("item"+i));
        }
        assertFalse(getList(instances, name).isEmpty());

//        Iterator iter = getList(instances, name).iterator();
//        int item = 0;
//        while (iter.hasNext()){
//            assertEquals("item"+item++, iter.next());
//        }

//        assertEquals(count, getList(instances, name).size());

        assertEquals("item0", getList(instances, name).get(0));
        assertEquals(count, getList(instances, name).size());
        getList(instances, name).add(0, "item");
        assertEquals(count+1, getList(instances, name).size());
        assertEquals("item", getList(instances, name).get(0));
        assertEquals("item0", getList(instances, name).get(1));
        assertTrue(getList(instances, name).remove("item99"));
        assertFalse(getList(instances, name).remove("item99"));
        assertEquals(count, getList(instances, name).size());
        assertEquals("item",getList(instances, name).set(0, "newItem"));
        assertEquals("newItem",getList(instances, name).get(0));

        getList(instances, name).clear();
        assertEquals(0, getList(instances, name).size());

        List list = new ArrayList();
        list.add("item-1");
        list.add("item-2");

        assertTrue(getList(instances, name).addAll(list));
        assertEquals("item-1", getList(instances, name).get(0));
        assertEquals("item-2", getList(instances, name).get(1));

        assertTrue(getList(instances, name).addAll(1,list));
        assertEquals("item-1", getList(instances, name).get(0));
        assertEquals("item-1", getList(instances, name).get(1));
        assertEquals("item-2", getList(instances, name).get(2));
        assertEquals("item-2", getList(instances, name).get(3));
        assertEquals(4, getList(instances, name).size());
        assertEquals(0, getList(instances, name).indexOf("item-1"));
        assertEquals(1, getList(instances, name).lastIndexOf("item-1"));
        assertEquals(2, getList(instances, name).indexOf("item-2"));
        assertEquals(3, getList(instances, name).lastIndexOf("item-2"));

        assertEquals(4, getList(instances, name).size());

        assertTrue(getList(instances, name).containsAll(list));
        list.add("asd");
        assertFalse(getList(instances, name).containsAll(list));
        assertTrue(getList(instances, name).contains("item-1"));
        assertFalse(getList(instances, name).contains("item"));

        list = getList(instances, name).subList(1, 3);

        assertEquals(2, list.size());
        assertEquals("item-1", list.get(0));
        assertEquals("item-2", list.get(1));

        final ListIterator listIterator = getList(instances, name).listIterator(1);
        assertTrue(listIterator.hasPrevious());
        assertEquals("item-1", listIterator.next());
        assertEquals("item-2", listIterator.next());
        assertEquals("item-2", listIterator.next());
        assertFalse(listIterator.hasNext());

        list = new ArrayList();
        list.add("item1");
        list.add("item2");

        assertFalse(getList(instances, name).removeAll(list));
        assertEquals(4, getList(instances, name).size());

        list.add("item-1");

        assertTrue(getList(instances, name).removeAll(list));
        assertEquals(2, getList(instances, name).size());

        list.clear();
        list.add("item-2");
        assertFalse(getList(instances, name).retainAll(list));
        assertEquals(2, getList(instances, name).size());

        list.set(0, "item");
        assertTrue(getList(instances, name).add("item"));
        assertTrue(getList(instances, name).retainAll(list));
        assertEquals(1, getList(instances, name).size());
        assertEquals("item", getList(instances, name).get(0));

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

    @Test
    public void testAddRemoveList(){
        Config config = new Config();
        final String name = "defList";

        final int insCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);
        TransactionContext context = instances[0].newTransactionContext();
        assertTrue(instances[1].getList(name).add("value1"));
        try {
            context.beginTransaction();

            TransactionalList l = context.getList(name);
            assertEquals(1, l.size());
            assertTrue(l.add("value1"));
            assertEquals(2, l.size());
            assertFalse(l.remove("value2"));
            assertEquals(2, l.size());
            assertTrue(l.remove("value1"));
            assertEquals(1, l.size());
            context.commitTransaction();
        } catch (Exception e){
            fail(e.getMessage());
            context.rollbackTransaction();
        }

        assertEquals(1, instances[1].getList(name).size());
    }

    @Test
    public void testMigration(){
        Config config = new Config();
        final String name = "defList";
        config.addListConfig(new ListConfig().setName(name).setBackupCount(1));

        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        IList list = instance1.getList(name);

        for (int i=0; i<100; i++){
            list.add("item"+i);
        }

        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        assertEquals(100, instance2.getList(name).size());

        HazelcastInstance instance3 = factory.newHazelcastInstance(config);
        assertEquals(100, instance3.getList(name).size());

        instance1.shutdown();
        assertEquals(100, instance3.getList(name).size());

        list = instance2.getList(name);
        for (int i=0; i<100; i++){
            list.add("item-"+i);
        }

        instance2.shutdown();
        assertEquals(200, instance3.getList(name).size());

        instance1 = factory.newHazelcastInstance(config);
        assertEquals(200, instance1.getList(name).size());

        instance3.shutdown();
        assertEquals(200, instance1.getList(name).size());

    }

    @Test
    public void testMaxSize(){
        Config config = new Config();
        final String name = "defList";
        config.addListConfig(new ListConfig().setName(name).setBackupCount(1).setMaxSize(100));

        final int insCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IList list = instance1.getList(name);

        for (int i=0; i<100; i++){
            assertTrue(list.add("item"+i));
        }
        assertFalse(list.add("item"));
        assertNotNull(list.remove(0));
        assertTrue(list.add("item"));
    }

    private IList getList(HazelcastInstance[] instances, String name){
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getList(name);
    }


}
