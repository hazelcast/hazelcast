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

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ValueCollectionTest {

    @Test
    public void testContainsAllEmptyCollection() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.containsAll(new ArrayList());
    }

    @Test
    public void testContainsAll() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Map map = new HashMap();
        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        ValueCollection valueCollection = new ValueCollection(entryHolder, map.entrySet());
        Collection c = new ArrayList();
        c.add("1");
        c.add("2");
        c.add("3");
        boolean result = valueCollection.containsAll(c);
        assertTrue(result);
    }

    @Test
    public void testContains() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Map map = new HashMap();
        map.put("1", "1");
        ValueCollection valueCollection = new ValueCollection(entryHolder, map.entrySet());
        boolean result = valueCollection.contains("1");
        assertTrue(result);
    }

    @Test
    public void testToArray() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Map map = new HashMap();
        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        ValueCollection valueCollection = new ValueCollection(entryHolder, map.entrySet());
        Object[] result = valueCollection.toArray();
        assertEquals(3, result.length);
        List l = Arrays.asList(result);
        assertTrue(l.containsAll(map.values()));
    }

    @Test
    public void testNotEmpty() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Map map = new HashMap();
        map.put("1", "value");
        ValueCollection valueCollection = new ValueCollection(entryHolder, map.entrySet());
        assertFalse(valueCollection.isEmpty());
    }

    @Test
    public void testEmpty() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        when(entryHolder.size()).thenReturn(0);
        Map map = new HashMap();
        ValueCollection valueCollection = new ValueCollection(entryHolder, map.entrySet());
        assertTrue(valueCollection.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.add("1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddAll() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.addAll(new ArrayList());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.remove("1");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAll() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.removeAll(null);
    }

    @Test
    public void testRetainAll() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        assertFalse(valueCollection.retainAll(new ArrayList()));
    }

    @Test
    public void testToArrayWithArgument() {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        set.add(new MapEntry("1", "1"));
        set.add(new MapEntry("2", "2"));
        set.add(new MapEntry("3", "3"));
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        {
            final Object[] values = valueCollection.toArray();
            Arrays.sort(values);
            assertArrayEquals(new Object[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = (String[]) valueCollection.toArray(new String[3]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = (String[]) valueCollection.toArray(new String[2]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = (String[]) valueCollection.toArray(new String[5]);
            Arrays.sort(values, 0, 3);
            assertArrayEquals(new String[]{"1", "2", "3", null, null}, values);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testToArrayWithArgumentNPE() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.toArray(null);
    }

    private static final class MapEntry<K, V> implements Map.Entry<K, V> {
        final K key;
        final V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }
}
