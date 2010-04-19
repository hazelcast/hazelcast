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

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        when(entryHolder.containsValue("1")).thenReturn(true);
        when(entryHolder.containsValue("2")).thenReturn(true);
        when(entryHolder.containsValue("3")).thenReturn(true);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
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
        when(entryHolder.containsValue("1")).thenReturn(true);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
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
        when(entryHolder.size()).thenReturn(1);
        Map map = new HashMap();
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

    @Test(expected = UnsupportedOperationException.class)
    public void testToArrayWithArgument() throws Exception {
        EntryHolder entryHolder = mock(EntryHolder.class);
        Set set = new HashSet();
        ValueCollection valueCollection = new ValueCollection(entryHolder, set);
        valueCollection.toArray(null);
    }
}
