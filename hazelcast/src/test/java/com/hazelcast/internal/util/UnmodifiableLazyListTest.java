/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static com.hazelcast.test.HazelcastTestSupport.assertContainsAll;
import static com.hazelcast.test.HazelcastTestSupport.assertIterableEquals;
import static com.hazelcast.test.HazelcastTestSupport.assertNotContains;
import static com.hazelcast.test.HazelcastTestSupport.assertNotContainsAll;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuickTest
@ParallelTest
@ParallelJVMTest
@SuppressWarnings({"rawtypes", "unchecked"})
public class UnmodifiableLazyListTest {

    private static final int SIZE = 10;
    private static SerializationService serializationService;
    private UnmodifiableLazyList list;

    @BeforeAll
    public static void setupSerializationService() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @BeforeEach
    public void setUp() {
        List<Data> dataList = new ArrayList<>();
        for (int i = 0; i < SIZE; i++) {
            dataList.add(serializationService.toData(i));
        }
        list = new UnmodifiableLazyList(dataList, serializationService);
    }

    @Test
    public void testIterator() {
        assertIterableEquals(list, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testIterator_remove() {
        Iterator<Object> iterator = list.iterator();
        iterator.next();
        assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
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
    @SuppressWarnings("SuspiciousToArrayCall")
    public void testToArray_AsGenericType_SmallSize() {
        Integer[] a = new Integer[randomInt()];
        Integer[] array = (Integer[]) list.toArray(a);
        assertParamsEquals(array, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    @SuppressWarnings("SuspiciousToArrayCall")
    public void testToArray_AsGenericType_GreaterSize() {
        Integer[] a = new Integer[SIZE + 1];
        Integer[] array = (Integer[]) list.toArray(a);
        assertParamsEquals(array, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, null);
    }

    @Test
    public void testAdd() {
        assertThatThrownBy(() -> list.add(randomInt())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testRemove() {
        assertThatThrownBy(() -> list.remove(Integer.valueOf(randomInt())))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testContainsAll_True() {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(randomInt());
        integers.add(randomInt());
        assertContainsAll(list, integers);
    }

    @Test
    public void testContainsAll_False() {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(randomInt());
        integers.add(randomInt() + SIZE);
        assertNotContainsAll(list, integers);
    }

    @Test
    public void testAddAll() {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(randomInt());
        integers.add(randomInt());
        assertThatThrownBy(() -> list.addAll(integers)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testRemoveAll() {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(randomInt());
        integers.add(randomInt());

        assertThatThrownBy(() -> list.removeAll(integers)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testRetainAll() {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(randomInt());
        integers.add(randomInt());
        assertThatThrownBy(() -> list.retainAll(integers)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testClear() {
        assertThatThrownBy(() -> list.clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testGet() {
        int i = randomInt();
        assertEquals(i, (int) list.get(i));
    }

    @Test
    public void testSet() {
        int i = randomInt();
        assertThatThrownBy(() -> list.set(i, i + 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testAdd_WithIndex() {
        int i = randomInt();
        assertThatThrownBy(() -> list.add(i, i + 1)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testRemove_WithIndex() {
        assertThatThrownBy(() -> list.remove(randomInt())).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testAddAll_WithIndex() {
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(randomInt());
        integers.add(randomInt());
        assertThatThrownBy(() -> list.addAll(randomInt(), integers))
            .isInstanceOf(UnsupportedOperationException.class);
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

    @Test
    public void testListIterator_Add() {
        ListIterator<Object> listIterator = list.listIterator();
        listIterator.next();

        assertThatThrownBy(() -> listIterator.add(randomInt()))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testListIterator_Remove() {
        ListIterator<Object> listIterator = list.listIterator();
        listIterator.next();

        assertThatThrownBy(listIterator::remove).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testListIterator_Set() {
        ListIterator<Object> listIterator = list.listIterator();
        listIterator.next();
        assertThatThrownBy(() -> listIterator.set(randomInt())).isInstanceOf(UnsupportedOperationException.class);
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
        assertArrayEquals(expected, array);
    }

    private int randomInt() {
        return RandomPicker.getInt(SIZE);
    }

}
