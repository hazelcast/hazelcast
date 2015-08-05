/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.HashUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LazyIteratorTest
        extends HazelcastTestSupport {

    private static final InternalReplicatedMapStorage<String, Integer> TEST_DATA_SIMPLE;
    private static final InternalReplicatedMapStorage<String, Integer> TEST_DATA_TOMBS;

    private static final ReplicatedRecordStore REPLICATED_RECORD_STORE = new NoOpReplicatedRecordStore();

    static {
        TEST_DATA_SIMPLE = new InternalReplicatedMapStorage<String, Integer>(new ReplicatedMapConfig());
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            int hash = HashUtil.hashCode(key);
            VectorClockTimestamp timestamp = new VectorClockTimestamp();
            TEST_DATA_SIMPLE.put(key, new ReplicatedRecord<String, Integer>(key, i, timestamp, hash, -1));
        }
        TEST_DATA_TOMBS = new InternalReplicatedMapStorage<String, Integer>(new ReplicatedMapConfig());
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            int hash = HashUtil.hashCode(key);
            VectorClockTimestamp timestamp = new VectorClockTimestamp();
            Integer value = i % 2 == 0 ? i : null;
            ReplicatedRecord<String, Integer> record = new ReplicatedRecord<String, Integer>(key, value, timestamp, hash, -1);
            TEST_DATA_TOMBS.put(key, record);
        }
    }

    @Test
    public void test_lazy_values_no_tombs_with_has_next()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);
        Iterator<Integer> iterator = collection.iterator();

        int count = 0;
        Set<Integer> values = new HashSet<Integer>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next());
        }
        assertEquals(100, count);
        assertEquals(100, values.size());
    }

    @Test
    public void test_lazy_values_no_tombs_with_has_next_every_second_time()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);
        Iterator<Integer> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                iterator.hasNext();
            }

            values.add(iterator.next());
        }
        assertEquals(100, values.size());
    }

    @Test
    public void test_lazy_values_no_tombs_more_elements_possible()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_SIMPLE);
        Iterator<Integer> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            values.add(iterator.next());
        }
        assertEquals(100, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_values_with_tombs_with_has_next()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_TOMBS);
        Iterator<Integer> iterator = collection.iterator();

        int count = 0;
        Set<Integer> values = new HashSet<Integer>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next());
        }
        assertEquals(50, count);
        assertEquals(50, values.size());
    }

    @Test
    public void test_lazy_values_with_tombs_with_next()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_TOMBS);
        Iterator<Integer> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            values.add(iterator.next());
        }
        assertEquals(50, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_values_with_tombs_copy()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_TOMBS);

        Set<Integer> copy = new HashSet<Integer>(collection);
        Iterator<Integer> iterator = copy.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            values.add(iterator.next());
        }
        assertEquals(50, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_values_with_tombs_to_array_new_array()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_TOMBS);

        Object[] array = collection.toArray();
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_values_with_tombs_to_array_passed_array_too_small()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_TOMBS);

        Integer[] array = collection.toArray(new Integer[0]);
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_values_with_tombs_to_array_passed_array_matching_size()
            throws Exception {

        ValuesIteratorFactory<String, Integer> factory = new ValuesIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazyCollection<String, Integer> collection = new LazyCollection<String, Integer>(factory, TEST_DATA_TOMBS);

        Integer[] array = collection.toArray(new Integer[50]);
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_keyset_no_tombs_with_has_next()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);
        Iterator<String> iterator = collection.iterator();

        int count = 0;
        Set<String> values = new HashSet<String>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next());
        }
        assertEquals(100, count);
        assertEquals(100, values.size());
    }

    @Test
    public void test_lazy_keyset_no_tombs_with_has_next_every_second_time()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);
        Iterator<String> iterator = collection.iterator();

        Set<String> values = new HashSet<String>();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                iterator.hasNext();
            }

            values.add(iterator.next());
        }
        assertEquals(100, values.size());
    }

    @Test
    public void test_lazy_keyset_no_tombs_more_elements_possible()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_SIMPLE);
        Iterator<String> iterator = collection.iterator();

        Set<String> values = new HashSet<String>();
        for (int i = 0; i < 100; i++) {
            values.add(iterator.next());
        }
        assertEquals(100, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_keyset_with_tombs_with_has_next()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_TOMBS);
        Iterator<String> iterator = collection.iterator();

        int count = 0;
        Set<String> values = new HashSet<String>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next());
        }
        assertEquals(50, count);
        assertEquals(50, values.size());
    }

    @Test
    public void test_lazy_keyset_with_tombs_with_next()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_TOMBS);
        Iterator<String> iterator = collection.iterator();

        Set<String> values = new HashSet<String>();
        for (int i = 0; i < 50; i++) {
            values.add(iterator.next());
        }
        assertEquals(50, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_keyset_with_tombs_copy()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_TOMBS);

        Set<String> copy = new HashSet<String>(collection);
        Iterator<String> iterator = copy.iterator();

        Set<String> values = new HashSet<String>();
        for (int i = 0; i < 50; i++) {
            values.add(iterator.next());
        }
        assertEquals(50, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_keyset_with_tombs_to_array_new_array()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_TOMBS);

        Object[] array = collection.toArray();
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_keyset_with_tombs_to_array_passed_array_too_small()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_TOMBS);

        String[] array = collection.toArray(new String[0]);
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_keyset_with_tombs_to_array_passed_array_matching_size()
            throws Exception {

        KeySetIteratorFactory<String, Integer> factory = new KeySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, String> collection = new LazySet<String, Integer, String>(factory, TEST_DATA_TOMBS);

        String[] array = collection.toArray(new String[50]);
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_entryset_no_tombs_with_has_next()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_SIMPLE);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        int count = 0;
        Set<Integer> values = new HashSet<Integer>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next().getValue());
        }
        assertEquals(100, count);
        assertEquals(100, values.size());
    }

    @Test
    public void test_lazy_entryset_no_tombs_with_has_next_every_second_time()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_SIMPLE);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                iterator.hasNext();
            }

            values.add(iterator.next().getValue());
        }
        assertEquals(100, values.size());
    }

    @Test
    public void test_lazy_entryset_no_tombs_more_elements_possible()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_SIMPLE);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 100; i++) {
            values.add(iterator.next().getValue());
        }
        assertEquals(100, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_entryset_with_tombs_with_has_next()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_TOMBS);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        int count = 0;
        Set<Integer> values = new HashSet<Integer>();
        while (iterator.hasNext()) {
            count++;
            values.add(iterator.next().getValue());
        }
        assertEquals(50, count);
        assertEquals(50, values.size());
    }

    @Test
    public void test_lazy_entryset_with_tombs_with_next()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_TOMBS);
        Iterator<Map.Entry<String, Integer>> iterator = collection.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            values.add(iterator.next().getValue());
        }
        assertEquals(50, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_entryset_with_tombs_copy()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_TOMBS);

        Set<Map.Entry<String, Integer>> copy = new HashSet<Map.Entry<String, Integer>>(collection);
        Iterator<Map.Entry<String, Integer>> iterator = copy.iterator();

        Set<Integer> values = new HashSet<Integer>();
        for (int i = 0; i < 50; i++) {
            values.add(iterator.next().getValue());
        }
        assertEquals(50, values.size());

        try {
            iterator.next();
            fail("Shouldn't have further elements!");
        } catch (NoSuchElementException e) {
            // We need to catch it here since we won't have a successful test
            // if any of the prior calls would throw it!
        }
    }

    @Test
    public void test_lazy_entryset_with_tombs_to_array_new_array()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_TOMBS);

        Object[] array = collection.toArray();
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_entryset_with_tombs_to_array_passed_array_too_small()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_TOMBS);

        Map.Entry<String, Integer>[] array = collection.toArray(new Map.Entry[0]);
        assertEquals(50, array.length);
    }

    @Test
    public void test_lazy_entryset_with_tombs_to_array_passed_array_matching_size()
            throws Exception {

        EntrySetIteratorFactory<String, Integer> factory = new EntrySetIteratorFactory<String, Integer>(REPLICATED_RECORD_STORE);
        LazySet<String, Integer, Map.Entry<String, Integer>> collection = //
                new LazySet<String, Integer, Map.Entry<String, Integer>>(factory, TEST_DATA_TOMBS);

        Map.Entry<String, Integer>[] array = collection.toArray(new Map.Entry[50]);
        assertEquals(50, array.length);
    }

    private static class NoOpReplicatedRecordStore
            implements ReplicatedRecordStore {

        @Override
        public String getName() {
            return null;
        }

        @Override
        public Object remove(Object key) {
            return null;
        }

        @Override
        public void evict(Object key) {

        }

        @Override
        public void removeTombstone(Object key) {

        }

        @Override
        public Object get(Object key) {
            return null;
        }

        @Override
        public Object put(Object key, Object value) {
            return null;
        }

        @Override
        public Object put(Object key, Object value, long ttl, TimeUnit timeUnit) {
            return null;
        }

        @Override
        public boolean containsKey(Object key) {
            return false;
        }

        @Override
        public boolean containsValue(Object value) {
            return false;
        }

        @Override
        public ReplicatedRecord getReplicatedRecord(Object key) {
            return null;
        }

        @Override
        public Set keySet() {
            return null;
        }

        @Override
        public Collection values() {
            return null;
        }

        @Override
        public Collection values(Comparator comparator) {
            return null;
        }

        @Override
        public Set entrySet() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void clear(boolean distribute, boolean emptyReplicationQueue) {
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Object unmarshallKey(Object key) {
            return key;
        }

        @Override
        public Object unmarshallValue(Object value) {
            return value;
        }

        @Override
        public Object marshallKey(Object key) {
            return key;
        }

        @Override
        public Object marshallValue(Object value) {
            return value;
        }

        @Override
        public String addEntryListener(EntryListener listener, Object key) {
            return null;
        }

        @Override
        public String addEntryListener(EntryListener listener, Predicate predicate, Object key) {
            return null;
        }

        @Override
        public boolean removeEntryListenerInternal(String id) {
            return false;
        }

        @Override
        public ReplicationPublisher getReplicationPublisher() {
            return null;
        }

        @Override
        public void destroy() {
        }
    }

}
