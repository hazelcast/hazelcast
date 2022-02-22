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

package com.hazelcast.internal.util;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.UnmodifiableLazyList;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnmodifiableLazyListTest extends HazelcastTestSupport {

    private static final int SIZE = 10;
    private static SerializationService serializationService;
    private UnmodifiableLazyList list;

    @BeforeClass
    public static void setupSerializationService() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Before
    public void setUp() {
        List<Data> dataList = new ArrayList<Data>();
        for (int i = 0; i < SIZE; i++) {
            dataList.add(serializationService.toData(i));
        }
        list = new UnmodifiableLazyList(dataList, serializationService);
    }

    @Test
    public void testIterator() {
        assertIterableEquals(list, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIterator_remove() {
        Iterator<Object> iterator = list.iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testSize() {
        assertEquals(SIZE, list.size());
    }

    @Test
    public void testIsEmpty_True() {
        list = new UnmodifiableLazyList(Collections.emptyList(), serializationService);
        assertTrue(list.isEmpty());
    }

    @Test
    public void testIsEmpty_False() {
        assertFalse(list.isEmpty());
    }

    @Test
    public void testContains_True() {
        assertContains(list, randomInt());
    }

    @Test
    public void testContains_False() {
        assertNotContains(list, randomInt() + SIZE);
    }

    @Test
    public void testToArray_AsObject() {
        Object[] array = list.toArray();
        assertParamsEquals(array, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testToArray_AsGenericType_SmallSize() {
        Integer[] a = new Integer[randomInt()];
        Integer[] array = (Integer[]) list.toArray(a);
        assertParamsEquals(array, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testToArray_AsGenericType_GreaterSize() {
        Integer[] a = new Integer[SIZE + 1];
        Integer[] array = (Integer[]) list.toArray(a);
        assertParamsEquals(array, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd() {
        list.add(randomInt());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        list.remove(new Integer(randomInt()));
    }

    @Test
    public void testContainsAll_True() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        integers.add(randomInt());
        integers.add(randomInt());
        assertContainsAll(list, integers);
    }

    @Test
    public void testContainsAll_False() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        integers.add(randomInt());
        integers.add(randomInt() + SIZE);
        assertNotContainsAll(list, integers);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddAll() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        integers.add(randomInt());
        integers.add(randomInt());
        list.addAll(integers);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAll() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        integers.add(randomInt());
        integers.add(randomInt());
        list.removeAll(integers);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetainAll() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        integers.add(randomInt());
        integers.add(randomInt());
        list.retainAll(integers);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClear() {
        list.clear();
    }

    @Test
    public void testGet() {
        int i = randomInt();
        assertEquals(i, (int) list.get(i));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSet() {
        int i = randomInt();
        list.set(i, i + 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd_WithIndex() {
        int i = randomInt();
        list.add(i, i + 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove_WithIndex() {
        list.remove(randomInt());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddAll_WithIndex() {
        ArrayList<Integer> integers = new ArrayList<Integer>();
        integers.add(randomInt());
        integers.add(randomInt());
        list.addAll(randomInt(), integers);
    }

    @Test
    public void testIndexOf() {
        int i = randomInt();
        assertEquals(i, list.indexOf(i));
    }

    @Test
    public void testLastIndexOf() {
        int i = randomInt();
        assertEquals(i, list.indexOf(i));
    }

    @Test
    public void testListIterator() {
        int i = randomInt();
        ListIterator listIterator = list.listIterator(i);
        while (listIterator.hasNext()) {
            assertEquals(i++, (int) listIterator.next());
        }
        while (listIterator.hasPrevious()) {
            assertEquals(--i, (int) listIterator.previous());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListIterator_Add() {
        ListIterator<Object> listIterator = list.listIterator();
        listIterator.next();
        listIterator.add(randomInt());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListIterator_Remove() {
        ListIterator<Object> listIterator = list.listIterator();
        listIterator.next();
        listIterator.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListIterator_Set() {
        ListIterator<Object> listIterator = list.listIterator();
        listIterator.next();
        listIterator.set(randomInt());
    }

    @Test
    public void testSubList() {
        int i = RandomPicker.getInt(SIZE / 2);
        List<Integer> subList = list.subList(i, i + SIZE / 2);
        for (int integer : subList) {
            assertEquals(i++, integer);
        }
    }

    @Test
    public void testSerialize() {
        Data data = serializationService.toData(list);
        List<Integer> deserializedList = serializationService.toObject(data);
        int size = list.size();
        assertEquals(size, deserializedList.size());
        for (int i = 0; i < size; i++) {
            assertEquals(list.get(i), deserializedList.get(i));
        }
    }

    private void assertParamsEquals(Object[] array, Object... expected) {
        assertArrayEquals(array, expected);
    }

    private int randomInt() {
        return RandomPicker.getInt(SIZE);
    }

}
