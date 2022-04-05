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

import com.hazelcast.config.Config;
import com.hazelcast.config.ListConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.collection.IList;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class ListAbstractTest extends HazelcastTestSupport {

    protected HazelcastInstance[] instances;
    protected IAtomicLong atomicLong;

    private IList<String> list;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.addListConfig(new ListConfig("testAdd_whenCapacityReached_thenItemNotAdded*").setMaxSize(10));

        instances = newInstances(config);
        HazelcastInstance local = instances[0];
        HazelcastInstance target = instances[instances.length - 1];
        String methodName = getTestMethodName();
        String name = randomNameOwnedBy(target, methodName);
        list = local.getList(name);
    }

    protected abstract HazelcastInstance[] newInstances(Config config);

    // ====================== IsEmpty ==================

    @Test
    public void testIsEmpty_whenEmpty() {
        assertTrue(list.isEmpty());
    }

    @Test
    public void testIsEmpty_whenNotEmpty() {
        list.add("1");
        assertFalse(list.isEmpty());
    }

    // =================== Add ===================

    @Test
    public void testAdd() {
        addItems(10);
        assertEquals(10, list.size());
    }

    @Test
    public void testAdd_whenArgNull() {
        try {
            list.add(null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }
        assertTrue(list.isEmpty());
    }

    @Test
    public void testAdd_whenCapacityReached_thenItemNotAdded() {
        for (int i = 0; i < 10; i++) {
            list.add("item" + i);
        }

        boolean added = list.add("item10");
        assertFalse(added);
        assertEquals(10, list.size());
    }

    @Test
    public void testAddWithIndex() {
        for (int i = 0; i < 10; i++) {
            list.add(i, "item" + i);
        }
        assertEquals(10, list.size());
    }

    @Test
    public void testAddWithIndex_whenIndexAlreadyTaken() {
        addItems(10);

        assertEquals("item4", list.get(4));
        list.add(4, "test");
        assertEquals("test", list.get(4));
    }

    @Test
    public void testAddWithIndex_whenIndexAlreadyTaken_ArgNull() {
        addItems(10);
        assertEquals("item4", list.get(4));
        try {
            list.add(4, null);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }
        assertEquals("item4", list.get(4));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testAddWithIndex_whenIndexOutOfBound() {
        addItems(10);
        list.add(14, "item14");
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testAddWithIndex_whenIndexNegative() {
        list.add(-1, "item0");
    }


    // ======================= AddAll =========================

    @Test
    public void testAddAll() {
        List<String> listTest = new ArrayList<String>();
        listTest.add("item0");
        listTest.add("item1");
        listTest.add("item2");

        assertTrue(list.addAll(listTest));
        assertEquals(3, list.size());
    }

    @Test
    public void testAddAll_whenCollectionContainsNull() {
        List<String> listTest = new ArrayList<String>();
        listTest.add("item0");
        listTest.add("item1");
        listTest.add(null);

        try {
            list.addAll(listTest);
            fail();
        } catch (NullPointerException expected) {
            ignore(expected);
        }
        assertEquals(0, list.size());
    }

    @Test
    public void testAddAll_whenEmptyCollection() {
        addItems(10);
        List<String> listTest = emptyList();
        assertEquals(10, list.size());
        assertFalse(list.addAll(listTest));
        assertEquals(10, list.size());
    }

    @Test
    public void testAddAll_whenDuplicateItems() {
        addItems(10);
        List<String> listTest = new ArrayList<String>();
        listTest.add("item4");
        list.addAll(listTest);
        assertEquals(11, list.size());
    }

    @Test
    public void testAddAllWithIndex() {
        addItems(10);
        List<String> listTest = new ArrayList<String>();
        listTest.add("test1");
        listTest.add("test2");
        listTest.add("test3");

        assertEquals("item1", list.get(1));
        assertEquals("item2", list.get(2));
        assertEquals("item3", list.get(3));
        assertTrue(list.addAll(1, listTest));
        assertEquals("test1", list.get(1));
        assertEquals("test2", list.get(2));
        assertEquals("test3", list.get(3));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testAddAllWithIndex_whenIndexNegative() {
        addItems(10);
        List<String> listTest = new ArrayList<String>();
        listTest.add("test1");
        list.addAll(-2, listTest);
    }

    // ====================== Clear =======================

    @Test
    public void testClear() {
        addItems(10);
        list.clear();
        assertEquals(0, list.size());
    }

    // ===================== Contains ========================

    @Test
    public void testContains() {
        addItems(10);
        assertContains(list, "item1");
        assertContains(list, "item5");
        assertContains(list, "item7");
        assertNotContains(list, "item11");
    }

    // ===================== ContainsAll =========================

    @Test
    public void testContainsAll() {
        addItems(10);
        List<String> listTest = new ArrayList<String>();
        listTest.add("item1");
        listTest.add("item4");
        listTest.add("item7");

        assertContainsAll(list, listTest);
    }

    @Test
    public void testContainsAll_whenListNotContains() {
        addItems(10);
        List<String> listTest = new ArrayList<String>();
        listTest.add("item1");
        listTest.add("item4");
        listTest.add("item14");

        assertNotContainsAll(list, listTest);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testContainsAll_whenCollectionNull() {
        addItems(10);
        List<String> listTest = null;
        list.containsAll(listTest);
    }

    // ===================== Get ========================

    @Test
    public void testGet() {
        addItems(10);
        assertEquals("item1", list.get(1));
        assertEquals("item7", list.get(7));
        assertEquals("item9", list.get(9));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGet_whenIndexNotExists() {
        addItems(10);
        list.get(14);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGet_whenIndexNegative() {
        list.get(-1);
    }

    // ========================= Set ==========================

    @Test
    public void testSet() {
        addItems(10);
        assertEquals("item1", list.set(1, "test1"));
        assertEquals("item3", list.set(3, "test3"));
        assertEquals("item8", list.set(8, "test8"));
        assertEquals("test1", list.get(1));
        assertEquals("test3", list.get(3));
        assertEquals("test8", list.get(8));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSet_whenListEmpty() {
        list.set(0, "item0");
    }

    @Test(expected = NullPointerException.class)
    public void testSet_whenElementNull() {
        list.set(0, null);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSet_whenIndexNegative() {
        list.set(-1, "item1");
    }

    // ========================= IndexOf =============================

    @Test
    public void testIndexOf() {
        addItems(10);
        assertEquals(0, list.indexOf("item0"));
        assertEquals(6, list.indexOf("item6"));
        assertEquals(9, list.indexOf("item9"));
        assertEquals(-1, list.indexOf("item15"));
        assertEquals(-1, list.indexOf("item67"));
    }

    @Test
    public void testIndexOf_whenDuplicateItems() {
        list.add("item1");
        list.add("item2");
        list.add("item3");
        list.add("item1");

        assertEquals(0, list.indexOf("item1"));
        assertNotEquals(3, list.indexOf("item1"));
    }

    @Test(expected = NullPointerException.class)
    public void testIndexOf_whenObjectNull() {
        addItems(10);
        list.indexOf(null);
    }

    // ====================== LastIndexOf ===================

    @Test
    public void testLastIndexOf() {
        list.add("item1");
        list.add("item2");
        list.add("item3");
        list.add("item1");
        list.add("item4");
        list.add("item1");

        assertEquals(5, list.lastIndexOf("item1"));
        assertNotEquals(0, list.lastIndexOf("item1"));
        assertNotEquals(3, list.lastIndexOf("item1"));
    }

    @Test(expected = NullPointerException.class)
    public void testLastIndexOf_whenObjectNull() {
        list.lastIndexOf(null);
    }

    // ================== Remove ====================

    @Test
    public void testRemoveIndex() {
        addItems(10);
        assertEquals("item0", list.remove(0));
        assertEquals("item4", list.remove(3));
        assertEquals("item7", list.remove(5));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRemoveIndex_whenIndexNegative() {
        list.remove(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRemoveIndex_whenIndexNotExists() {
        addItems(10);
        list.remove(14);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRemoveIndex_whenListEmpty() {
        list.remove(0);
    }

    @Test
    public void testRemoveObject() {
        list.add("item0");
        list.add("item1");
        list.add("item2");
        list.add("item0");

        assertTrue(list.remove("item0"));
        assertFalse(list.remove("item3"));
        assertEquals("item1", list.get(0));
        assertEquals("item0", list.get(2));
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveObject_whenObjectNull() {
        list.remove(null);
    }

    @Test
    public void testRemoveObject_whenListEmpty() {
        assertFalse(list.remove("item0"));
    }

    // ====================== RemoveAll ======================

    @Test
    public void testRemoveAll() {
        addItems(10);
        List<String> listTest = new ArrayList<String>();
        listTest.add("item0");
        listTest.add("item1");
        listTest.add("item2");

        assertTrue(list.removeAll(listTest));
        assertEquals(7, list.size());
        assertEquals("item3", list.get(0));
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testRemoveAll_whenCollectionNull() {
        list.removeAll(null);
    }

    @Test
    public void testRemoveAll_whenCollectionEmpty() {
        addItems(10);
        List<String> listTest = emptyList();
        assertFalse(list.removeAll(listTest));
        assertEquals(10, list.size());
    }

    // ======================= RetainAll =======================

    @Test
    public void testRetainAll() {
        addItems(10);

        List<String> listTest = new ArrayList<String>();
        listTest.add("item0");
        listTest.add("item1");
        listTest.add("item2");

        assertTrue(list.retainAll(listTest));
        assertEquals(3, list.size());
        assertIterableEquals(list, "item0", "item1", "item2");
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("ConstantConditions")
    public void testRetainAll_whenCollectionNull() {
        list.retainAll(null);
    }

    @Test
    public void testRetainAll_whenCollectionEmpty() {
        addItems(10);
        List<String> listTest = emptyList();
        assertTrue(list.retainAll(listTest));
        assertEquals(0, list.size());
    }

    @Test(expected = NullPointerException.class)
    public void testRetainAll_whenCollectionContainsNull() {
        List<String> listTest = new ArrayList<String>();
        listTest.add(null);
        list.retainAll(listTest);
    }

    // ===================== SubList ========================

    @Test
    public void testSublist() {
        addItems(10);
        List listTest = list.subList(3, 7);
        assertEquals(4, listTest.size());
        assertIterableEquals(listTest, "item3", "item4", "item5", "item6");
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSublist_whenFromIndexIllegal() {
        list.subList(8, 7);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSublist_whenToIndexIllegal() {
        addItems(10);
        list.subList(4, 14);
    }

    // ================== Iterator ====================

    @Test
    public void testIterator() {
        addItems(10);
        ListIterator iterator = list.listIterator();
        int i = 0;
        while (iterator.hasNext()) {
            Object o = iterator.next();
            assertEquals(o, "item" + i++);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator_throwsException_whenRemove() {
        addItems(10);
        ListIterator iterator = list.listIterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testIteratorWithIndex() {
        addItems(10);
        int i = 4;
        ListIterator iterator = list.listIterator(i);
        while (iterator.hasNext()) {
            Object o = iterator.next();
            assertEquals(o, "item" + i++);
        }
    }

    // ======================== methods ==========================

    @SuppressWarnings("SameParameterValue")
    private void addItems(int count) {
        for (int i = 0; i < count; i++) {
            list.add("item" + i);
        }
    }
}
