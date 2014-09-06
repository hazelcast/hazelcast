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
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
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
 * @author ali 3/6/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SetTest extends HazelcastTestSupport {

    @Test
    public void testSetMethods() throws Exception {
        Config config = new Config();
        final String name = "defSet";
        final int count = 100;
        final int insCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);
        final HazelcastInstance[] instances = factory.newInstances(config);

        for (int i=0; i<count; i++){
            assertTrue(getSet(instances, name).add("item"+i));
        }
        assertFalse(getSet(instances, name).add("item0"));

        Iterator iter = getSet(instances, name).iterator();
        int item = 0;
        while (iter.hasNext()){
            getSet(instances, name).contains(iter.next());
            item++;
        }
        assertEquals(count, item);

        assertEquals(count, getSet(instances, name).size());

        assertTrue(getSet(instances, name).remove("item99"));
        assertFalse(getSet(instances, name).remove("item99"));

        List list = new ArrayList();
        list.add("item-1");
        list.add("item-2");

        assertTrue(getSet(instances, name).addAll(list));
        assertEquals(count+list.size()-1, getSet(instances, name).size());
        assertFalse(getSet(instances, name).addAll(list));
        assertEquals(count+list.size()-1, getSet(instances, name).size());

        assertTrue(getSet(instances, name).containsAll(list));
        list.add("asd");
        assertFalse(getSet(instances, name).containsAll(list));
        assertTrue(getSet(instances, name).contains("item98"));
        assertFalse(getSet(instances, name).contains("item99"));
    }

    @Test
    public void testListener() throws Exception {
        Config config = new Config();
        final String name = "defSet";
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

        getSet(instances, name).addItemListener(listener, true);

        for (int i = 0; i < count; i++) {
            assertTrue(getSet(instances, name).add("item" + i));
        }
        for (int i = 0; i < count; i++) {
            assertTrue(getSet(instances, name).remove("item" + i));
        }
        assertTrue(latchAdd.await(5, TimeUnit.SECONDS));
        assertTrue(latchRemove.await(5, TimeUnit.SECONDS));

    }

    @Test
    public void testMigration(){
        final String name = "defSet";

        final int insCount = 4;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);

        HazelcastInstance instance1 = factory.newHazelcastInstance();

        ISet set = instance1.getSet(name);

        for (int i=0; i<100; i++){
            set.add("item" + i);
        }

        HazelcastInstance instance2 = factory.newHazelcastInstance();
        assertEquals(100, instance2.getSet(name).size());

        HazelcastInstance instance3 = factory.newHazelcastInstance();
        assertEquals(100, instance3.getSet(name).size());

        instance1.shutdown();
        assertEquals(100, instance3.getSet(name).size());

        set = instance2.getSet(name);
        for (int i=0; i<100; i++){
            set.add("item-" + i);
        }

        instance2.shutdown();
        assertEquals(200, instance3.getSet(name).size());

        instance1 = factory.newHazelcastInstance();
        assertEquals(200, instance1.getSet(name).size());

        instance3.shutdown();
        assertEquals(200, instance1.getSet(name).size());
    }

    @Test
    public void testMaxSize(){
        Config config = new Config();
        final String name = "defSet";
        config.addSetConfig(new SetConfig().setName(name).setBackupCount(1).setMaxSize(100));

        final int insCount = 2;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(insCount);

        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        ISet set = instance1.getSet(name);

        for (int i=0; i<100; i++){
            assertTrue(set.add("item" + i));
        }
        assertFalse(set.add("item"));
        assertNotNull(set.remove("item0"));
        assertTrue(set.add("item"));
    }

    @Test(expected = UnsupportedOperationException.class)
    @ClientCompatibleTest
    public void testIteratorRemoveThrowsUnsupportedOperationException() {
        Config config = new Config();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);

        ISet set = instance1.getSet("defSet");
        set.add("item");

        Iterator iterator = set.iterator();
        iterator.next();
        iterator.remove();
    }

    private ISet getSet(HazelcastInstance[] instances, String name){
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getSet(name);
    }
}
