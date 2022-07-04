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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class LazyIteratorTest extends HazelcastTestSupport {

    private static final InternalReplicatedMapStorage<String, Integer> TEST_DATA_SIMPLE;

    static {
        TEST_DATA_SIMPLE = new InternalReplicatedMapStorage<String, Integer>();
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            TEST_DATA_SIMPLE.put(key, new ReplicatedRecord<String, Integer>(key, i, -1));
            TEST_DATA_SIMPLE.incrementVersion();
        }
    }

    private ReplicatedRecordStore replicatedRecordStore;

    @Before
    public void setUp() {
        // mocks a ReplicatedRecordStore, which does nothing beside returning the given key on (un)marshalling
        replicatedRecordStore = mock(ReplicatedRecordStore.class);
        when(replicatedRecordStore.marshall(anyObject())).thenAnswer(new ReturnFirstArgumentAnswer());
        when(replicatedRecordStore.unmarshall(anyObject())).thenAnswer(new ReturnFirstArgumentAnswer());
    }

    @Test
    public void testLazyCollection_size() {
        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(replicatedRecordStore);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);

        assertEqualsStringFormat("Expected %d items in LazyCollection, but was %d", 100, collection.size());
    }

    @Test
    public void testLazyCollection_isEmpty() {
        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(replicatedRecordStore);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);

        assertFalse("Expected LazyCollection to no be empty", collection.isEmpty());
    }

    @Test
    public void testLazyCollection_withValuesIterator_hasNext() {
        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(replicatedRecordStore);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);
        Iterator<Integer> iterator = collection.iterator();

        int count = 0;
        Set<Integer> values = new HashSet<Integer>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next());
        }
        assertEqualsStringFormat("Expected %d items in the LazyCollection.iterator(), but was %d", 100, count);
        assertEqualsStringFormat("Expected %d unique items in the LazyCollection.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazyCollection.iterator()", iterator.hasNext());
    }

    @Test
    public void testLazyCollection_withValuesIterator_hasNext_everySecondTime() {
        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(replicatedRecordStore);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);
        Iterator<Integer> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                assertTrue("Expected more items in LazyCollection.iterator()", iterator.hasNext());
            }
            values.add(iterator.next());
        }
        assertEqualsStringFormat("Expected %d unique items in the LazyCollection.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazyCollection.iterator()", iterator.hasNext());
    }

    @Test
    public void testLazyCollection_withValuesIterator_next_whenNoMoreElementsAreAvailable() {
        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(replicatedRecordStore);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);
        Iterator<Integer> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            values.add(iterator.next());
        }
        assertEqualsStringFormat("Expected %d unique items in the LazyCollection.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazyCollection.iterator()", iterator.hasNext());
        try {
            iterator.next();
            fail("LazyCollection.iterator() shouldn't have further items");
        } catch (NoSuchElementException expected) {
            // we need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void testLazySet_size() {
        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, String> set = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);

        assertEqualsStringFormat("Expected %d items in LazySet, but was %d", 100, set.size());
    }

    @Test
    public void testLazySet_isEmpty() {
        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, String> set = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);

        assertFalse("Expected LazySet to no be empty", set.isEmpty());
    }

    @Test
    public void testLazySet_withKeySetIterator_hasNext() {
        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);
        Iterator<String> iterator = collection.iterator();

        int count = 0;
        Set<String> values = new HashSet<String>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next());
        }
        assertEqualsStringFormat("Expected %d items in the LazySet.iterator(), but was %d", 100, count);
        assertEqualsStringFormat("Expected %d unique items in the LazySet.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazySet.iterator()", iterator.hasNext());
    }

    @Test
    public void testLazySet_withKeySetIterator_hasNext_everySecondTime() {
        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);
        Iterator<String> iterator = collection.iterator();

        Set<String> values = new HashSet<String>();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                assertTrue("Expected more items in LazySet.iterator()", iterator.hasNext());
            }
            values.add(iterator.next());
        }
        assertEqualsStringFormat("Expected %d unique items in the LazySet.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazySet.iterator()", iterator.hasNext());
    }

    @Test
    public void testLazySet_withKeySetIterator_next_whenNoMoreElementsAreAvailable() {
        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);
        Iterator<String> iterator = collection.iterator();

        Set<String> values = new HashSet<String>();
        for (int i = 0; i < 100; i++) {
            values.add(iterator.next());
        }
        assertEqualsStringFormat("Expected %d unique items in the LazySet.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazySet.iterator()", iterator.hasNext());
        try {
            iterator.next();
            fail("LazySet.iterator() shouldn't have further items");
        } catch (NoSuchElementException expected) {
            // we need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void testLazySet_withEntrySetIterator_hasNext() {
        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection
                = new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_SIMPLE);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        int count = 0;
        Set<Integer> values = new HashSet<Integer>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next().getValue());
        }
        assertEqualsStringFormat("Expected %d items in the LazySet.iterator(), but was %d", 100, count);
        assertEqualsStringFormat("Expected %d unique items in the LazySet.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazySet.iterator()", iterator.hasNext());
    }

    @Test
    public void testLazySet_withEntrySetIterator_hasNext_everySecondTime() {
        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection
                = new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_SIMPLE);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                assertTrue("Expected more items in LazySet.iterator()", iterator.hasNext());
            }
            values.add(iterator.next().getValue());
        }
        assertEqualsStringFormat("Expected %d unique items in the LazySet.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazySet.iterator()", iterator.hasNext());
    }

    @Test
    public void testLazySet_withEntrySetIterator_next_whenNoMoreElementsAreAvailable() {
        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(replicatedRecordStore);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection
                = new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_SIMPLE);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            values.add(iterator.next().getValue());
        }
        assertEqualsStringFormat("Expected %d unique items in the LazySet.iterator(), but was %d", 100, values.size());
        assertFalse("Expected no more items in LazySet.iterator()", iterator.hasNext());
        try {
            iterator.next();
            fail("LazySet.iterator() shouldn't have further items");
        } catch (NoSuchElementException expected) {
            // we need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    private static class ReturnFirstArgumentAnswer implements Answer<Object> {
        @Override
        public Object answer(InvocationOnMock invocation) {
            return invocation.getArguments()[0];
        }
    }
}
