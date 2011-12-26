/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HazelcastClientListTest extends HazelcastClientTestBase {

    @Test(expected = NullPointerException.class)
    public void addNull() {
        HazelcastClient hClient = getHazelcastClient();
        IList<?> list = hClient.getList("addNull");
        list.add(null);
    }

    @Test
    public void getListName() {
        HazelcastClient hClient = getHazelcastClient();
        IList<?> list = hClient.getList("getListName");
        assertEquals("getListName", list.getName());
    }

    @Test
    @Ignore
    public void addRemoveItemListener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final IList<String> list = hClient.getList("addRemoveItemListenerList");
        final CountDownLatch addLatch = new CountDownLatch(4);
        final CountDownLatch removeLatch = new CountDownLatch(4);
        ItemListener<String> listener = new CountDownItemListener<String>(addLatch, removeLatch);
        list.addItemListener(listener, true);
        list.add("hello");
        list.add("hello");
        list.remove("hello");
        list.remove("hello");
        list.removeItemListener(listener);
        list.add("hello");
        list.add("hello");
        list.remove("hello");
        list.remove("hello");
        Thread.sleep(10);
        assertEquals(2, addLatch.getCount());
        assertEquals(2, removeLatch.getCount());
    }

    @Test
    public void testListItemListener() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        String name = "testListListener";
        listener(latch, getHazelcastInstance().<String>getList(name), getHazelcastClient().<String>getList(name));
    }

    @Test
    public void testListItemListenerOtherWay() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        String name = "testListListener";
        listener(latch, getHazelcastClient().<String>getList(name), getHazelcastInstance().<String>getList(name));
    }

    private void listener(final CountDownLatch latch, IList<String> listOperation, IList<String> listListener) {
        listListener.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }
        }, true);
        listOperation.add("hello");
        listOperation.remove("hello");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test(timeout = 2000)
    public void testListAddFromServerGetFromClient() {
        HazelcastInstance h = getHazelcastInstance();
        HazelcastInstance client = getHazelcastClient();
        String name = "testListAddFromServerGetFromClient";
        h.getList(name).add("message");
        assertTrue(client.getList(name).contains("message"));
    }

    @Test
    public void destroy() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("destroy");
        for (int i = 0; i < 100; i++) {
            assertTrue(list.add(i));
        }
        IList<Integer> list2 = hClient.getList("destroy");
        assertTrue(list == list2);
        assertTrue(list.getId().equals(list2.getId()));
        list.destroy();
        list2 = hClient.getList("destroy");
        assertFalse(list == list2);
    }

    @Test
    public void add() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("add");
        int count = 100;
        for (int i = 0; i < count; i++) {
            assertTrue(list.add(i));
        }
    }

    @Test
    public void contains() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("contains");
        int count = 100;
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
        for (int i = 0; i < count; i++) {
            assertTrue(list.contains(i));
        }
        for (int i = count; i < 2 * count; i++) {
            assertFalse(list.contains(i));
        }
    }

    @Test
    public void addAll() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("addAll");
        List<Integer> arrList = new ArrayList<Integer>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
    }

    @Test
    public void containsAll() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("containsAll");
        List<Integer> arrList = new ArrayList<Integer>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
        assertTrue(list.containsAll(arrList));
        arrList.set((int) count / 2, count + 1);
        assertFalse(list.containsAll(arrList));
    }

    @Test
    public void size() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("size");
        int count = 100;
        assertTrue(list.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(list.add(i));
        }
        assertEquals(count, list.size());
        for (int i = 0; i < count / 2; i++) {
            assertTrue(list.add(i));
        }
        assertFalse(list.isEmpty());
        assertEquals(count + count / 2, list.size());
    }

    @Test
    public void remove() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("remove");
        int count = 100;
        assertTrue(list.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(list.add(i));
        }
        assertEquals(count, list.size());
        for (int i = 0; i < count; i++) {
            assertTrue(list.remove((Object) i));
        }
        assertTrue(list.isEmpty());
        for (int i = count; i < 2 * count; i++) {
            assertFalse(list.remove((Object) i));
        }
    }

    @Test
    public void clear() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("clear");
        int count = 100;
        assertTrue(list.isEmpty());
        for (int i = 0; i < count; i++) {
            assertTrue(list.add(i));
        }
        assertEquals(count, list.size());
        list.clear();
        assertTrue("List is not empty " + list.size(), list.isEmpty());
    }

    @Test
    public void removeAll() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("removeAll");
        List<Integer> arrList = new ArrayList<Integer>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            arrList.add(i);
        }
        assertTrue(list.addAll(arrList));
        assertTrue(list.removeAll(arrList));
        assertFalse(list.removeAll(arrList.subList(0, count / 10)));
    }

    @Test
    public void iterate() {
        HazelcastClient hClient = getHazelcastClient();
        IList<Integer> list = hClient.getList("iterate");
        list.add(1);
        list.add(2);
        list.add(2);
        list.add(3);
        assertEquals(4, list.size());
        Map<Integer, Integer> counter = new HashMap<Integer, Integer>();
        counter.put(1, 1);
        counter.put(2, 2);
        counter.put(3, 1);
        for (Iterator<Integer> iterator = list.iterator(); iterator.hasNext();) {
            Integer integer = iterator.next();
            counter.put(integer, counter.get(integer) - 1);
            iterator.remove();
        }
        assertEquals(Integer.valueOf(0), counter.get(1));
        assertEquals(Integer.valueOf(0), counter.get(2));
        assertEquals(Integer.valueOf(0), counter.get(3));
        assertTrue(list.isEmpty());
    }
}
