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

package com.hazelcast.client;

import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class HazelcastClientSetTest extends HazelcastClientTestBase {

    @Test(expected = NullPointerException.class)
    public void testAddNull() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        ISet<?> set = hClient.getSet("testAddNull");
        set.add(null);
    }

    @Test
    public void getSetName() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<String> set = hClient.getSet("getSetName");
        assertEquals("getSetName", set.getName());
    }

    @Test
    public void addRemoveItemListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final ISet<String> set = hClient.getSet("addRemoveItemListenerSet");
        final CountDownLatch addLatch = new CountDownLatch(2);
        final CountDownLatch removeLatch = new CountDownLatch(2);
        ItemListener<String> listener = new CountDownItemListener<String>(addLatch, removeLatch);
        final String id = set.addItemListener(listener, true);
        Thread.sleep(100);
        set.add("hello");
        set.add("hello");
        set.remove("hello");
        set.remove("hello");
        for (int i = 0; i < 100; i++) {
            if (removeLatch.getCount() != 1 || addLatch.getCount() != 1) {
                Thread.sleep(50);
            } else {
                break;
            }
        }
        assertEquals(1, removeLatch.getCount());
        assertEquals(1, addLatch.getCount());
        set.removeItemListener(id);
        set.add("hello");
        set.add("hello");
        set.remove("hello");
        set.remove("hello");
        Thread.sleep(50);
        assertEquals(1, addLatch.getCount());
        assertEquals(1, removeLatch.getCount());
    }

    @Test
    @Ignore
    public void TenTimesAddRemoveItemListener() throws InterruptedException {
        ExecutorService ex = Executors.newFixedThreadPool(1);
        final int count = 10;
        final CountDownLatch latch = new CountDownLatch(count);
        ex.execute(new Runnable() {
            public void run() {
                for (int i = 0; i < count; i++) {
                    try {
                        System.out.println("i: " + i);
                        addRemoveItemListener();
                        latch.countDown();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }
        });
        assertTrue(latch.await(20, TimeUnit.SECONDS));
    }

    @Test
    public void destroy() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("destroy");
        for (int i = 0; i < 100; i++) {
            assertTrue(set.add(i));
        }
        ISet<Integer> set2 = hClient.getSet("destroy");
        assertTrue(set == set2);
        assertTrue(set.getId().equals(set2.getId()));
        set.destroy();
        set2 = hClient.getSet("destroy");
        assertFalse(set == set2);
//        for(int i=0;i<100;i++){
//        	assertNull(list2.get(i));
//        }
    }

    @Test
    public void add() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("add");
        int count = 100;
        for (int i = 0; i < count; i++) {
            assertTrue(set.add(i));
        }
        for (int i = 0; i < count; i++) {
            assertFalse(set.add(i));
        }
        assertEquals(count, set.size());
    }

    @Test
    public void contains() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("contains");
        int count = 100;
        for (int i = 0; i < count; i++) {
            set.add(i);
        }
        for (int i = 0; i < count; i++) {
            assertTrue(set.contains(i));
        }
        for (int i = count; i < 2 * count; i++) {
            assertFalse(set.contains(i));
        }
    }

    @Test
    public void addAll() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("addAll");
        List<Integer> arr = new ArrayList<Integer>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            arr.add(i);
        }
        assertTrue(set.addAll(arr));
    }

    @Test
    public void containsAll() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("containsAll");
        List<Integer> arrList = new ArrayList<Integer>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            arrList.add(i);
        }
        assertTrue(set.addAll(arrList));
        assertTrue(set.containsAll(arrList));
        arrList.set((int) count / 2, count + 1);
        assertFalse(set.containsAll(arrList));
    }

    @Test
    public void size() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("size");
        int count = 100;
        assertTrue(set.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(set.add(i));
        }
        assertEquals(count, set.size());
        for (int i = 0; i < count / 2; i++) {
            assertFalse(set.add(i));
        }
        assertFalse(set.isEmpty());
        assertEquals(count, set.size());
    }

    @Test
    public void remove() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("remove");
        int count = 100;
        assertTrue(set.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(set.add(i));
        }
        assertEquals(count, set.size());
        for (int i = 0; i < count; i++) {
            assertTrue(set.remove((Object) i));
        }
        assertTrue(set.isEmpty());
        for (int i = count; i < 2 * count; i++) {
            assertFalse(set.remove((Object) i));
        }
    }

    @Test
    public void clear() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("clear");
        int count = 100;
        assertTrue(set.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(set.add(i));
        }
        assertEquals(count, set.size());
        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void removeAll() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("removeAll");
        List<Integer> arrList = new ArrayList<Integer>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            arrList.add(i);
        }
        assertTrue(set.addAll(arrList));
        assertTrue(set.removeAll(arrList));
        assertFalse(set.removeAll(arrList.subList(0, count / 10)));
    }

    @Test
    public void iterate() {
        HazelcastClient hClient = getHazelcastClient();
        ISet<Integer> set = hClient.getSet("iterate");
        set.add(1);
        set.add(2);
        set.add(2);
        set.add(3);
        assertEquals(3, set.size());
        Map<Integer, Integer> counter = new HashMap<Integer, Integer>();
        counter.put(1, 1);
        counter.put(2, 1);
        counter.put(3, 1);
        for (Iterator<Integer> iterator = set.iterator(); iterator.hasNext(); ) {
            Integer integer = iterator.next();
            counter.put(integer, counter.get(integer) - 1);
            iterator.remove();
        }
        assertEquals(Integer.valueOf(0), counter.get(1));
        assertEquals(Integer.valueOf(0), counter.get(2));
        assertEquals(Integer.valueOf(0), counter.get(3));
        assertTrue(set.isEmpty());
    }

    @AfterClass
    public static void shutdown() {
    }
}
