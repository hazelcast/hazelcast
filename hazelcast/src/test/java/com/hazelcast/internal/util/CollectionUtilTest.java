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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.CollectionUtil.addToValueList;
import static com.hazelcast.internal.util.CollectionUtil.getItemAtPositionOrNull;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.internal.util.CollectionUtil.nullToEmpty;
import static com.hazelcast.internal.util.CollectionUtil.objectToDataCollection;
import static com.hazelcast.internal.util.CollectionUtil.toIntArray;
import static com.hazelcast.internal.util.CollectionUtil.asIntegerList;
import static com.hazelcast.internal.util.CollectionUtil.toLongArray;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CollectionUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(CollectionUtil.class);
    }

    @Test
    public void testIsEmpty() {
        assertTrue(isEmpty(EMPTY_LIST));
        assertFalse(isEmpty(singletonList(23)));
    }

    @Test
    public void testIsNotEmpty() {
        assertFalse(isNotEmpty(EMPTY_LIST));
        assertTrue(isNotEmpty(singletonList(23)));
    }

    @Test
    public void testAddToValueList() {
        List<Integer> list = new ArrayList<Integer>();
        list.add(23);

        Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();
        map.put("existingKey", list);

        addToValueList(map, "existingKey", 42);

        assertEquals(1, map.size());
        assertEquals(2, list.size());
        assertContains(list, 42);
    }

    @Test
    public void testAddToValueList_whenKeyDoesNotExist_thenNewListIsCreated() {
        Map<String, List<Integer>> map = new HashMap<String, List<Integer>>();

        addToValueList(map, "nonExistingKey", 42);

        assertEquals(1, map.size());

        List<Integer> list = map.get("nonExistingKey");
        assertEquals(1, list.size());
        assertContains(list, 42);
    }

    @Test
    public void testGetItemAtPositionOrNull_whenEmptyArray_thenReturnNull() {
        Collection<Object> src = new LinkedHashSet<Object>();
        Object result = getItemAtPositionOrNull(src, 0);

        assertNull(result);
    }

    @Test
    public void testGetItemAtPositionOrNull_whenPositionExist_thenReturnTheItem() {
        Object obj = new Object();
        Collection<Object> src = new LinkedHashSet<Object>();
        src.add(obj);

        Object result = getItemAtPositionOrNull(src, 0);

        assertSame(obj, result);
    }

    @Test
    public void testGetItemAtPositionOrNull_whenSmallerArray_thenReturnNull() {
        Object obj = new Object();
        Collection<Object> src = new LinkedHashSet<Object>();
        src.add(obj);

        Object result = getItemAtPositionOrNull(src, 1);

        assertNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetItemAsPositionOrNull_whenInputImplementsList_thenDoNotUserIterator() {
        Object obj = new Object();

        List<Object> src = mock(List.class);
        when(src.size()).thenReturn(1);
        when(src.get(0)).thenReturn(obj);

        Object result = getItemAtPositionOrNull(src, 0);

        assertSame(obj, result);
        verify(src, never()).iterator();
    }

    @Test
    public void testObjectToDataCollection_size() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Collection<Object> list = new ArrayList<Object>();
        list.add(1);
        list.add("foo");

        Collection<Data> dataCollection = objectToDataCollection(list, serializationService);

        assertEquals(list.size(), dataCollection.size());
    }

    @Test(expected = NullPointerException.class)
    public void testObjectToDataCollection_withNullItem() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Collection<Object> list = new ArrayList<Object>();
        list.add(null);

        objectToDataCollection(list, serializationService);
    }

    @Test(expected = NullPointerException.class)
    public void testObjectToDataCollection_withNullCollection() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        objectToDataCollection(null, serializationService);
    }

    @Test
    public void testObjectToDataCollection_deserializeBack() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Collection<Object> list = new ArrayList<Object>();
        list.add(1);
        list.add("foo");

        Collection<Data> dataCollection = objectToDataCollection(list, serializationService);

        Iterator<Data> it1 = dataCollection.iterator();
        Iterator it2 = list.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            assertEquals(serializationService.toObject(it1.next()), it2.next());
        }
    }

    @Test
    public void testToIntArray() {
        List<Integer> list = new ArrayList<Integer>();
        list.add(42);
        list.add(23);
        list.add(Integer.MAX_VALUE);

        int[] intArray = toIntArray(list);

        assertNotNull(intArray);
        assertEquals(list.size(), intArray.length);
        assertEquals(list.get(0).intValue(), intArray[0]);
        assertEquals(list.get(1).intValue(), intArray[1]);
        assertEquals(list.get(2).intValue(), intArray[2]);
    }

    @Test(expected = NullPointerException.class)
    public void testToIntArray_whenNull_thenThrowNPE() {
        toIntArray(null);
    }

    @Test
    public void testToLongArray() {
        List<Long> list = new ArrayList<Long>();
        list.add(42L);
        list.add(23L);
        list.add(Long.MAX_VALUE);

        long[] longArray = toLongArray(list);

        assertNotNull(longArray);
        assertEquals(list.size(), longArray.length);
        assertEquals(list.get(0).longValue(), longArray[0]);
        assertEquals(list.get(1).longValue(), longArray[1]);
        assertEquals(list.get(2).longValue(), longArray[2]);
    }

    @Test(expected = NullPointerException.class)
    public void testToLongArray_whenNull_thenThrowNPE() {
        toLongArray(null);
    }

    @Test(expected = NullPointerException.class)
    public void testToIntegerList_whenNull() {
        asIntegerList(null);
    }

    @Test
    public void testToIntegerList_whenEmpty() {
        List<Integer> result = asIntegerList(new int[0]);
        assertEquals(0, result.size());
    }

    @Test
    public void testToIntegerList_whenNotEmpty() {
        List<Integer> result = asIntegerList(new int[]{1, 2, 3, 4});
        assertEquals(asList(1, 2, 3, 4), result);
    }

    @Test
    public void testNullToEmpty_whenNull() {
        assertEquals(emptyList(), nullToEmpty(null));
    }

    @Test
    public void testNullToEmpty_whenNotNull() {
        List<Integer> result = asList(1, 2, 3, 4, 5);
        assertEquals(result, nullToEmpty(result));
    }
}
