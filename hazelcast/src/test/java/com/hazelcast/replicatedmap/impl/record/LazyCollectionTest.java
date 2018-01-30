/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class LazyCollectionTest {

    private LazyCollection<Object, Object> collection;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        ValuesIteratorFactory<Object, Object> iteratorFactory = mock(ValuesIteratorFactory.class);
        InternalReplicatedMapStorage<Object, Object> storage = mock(InternalReplicatedMapStorage.class);
        collection = new LazyCollection<Object, Object>(iteratorFactory, storage);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_throws_exception() {
        collection.add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_all_throws_exception() {
        collection.addAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove_throws_exception() {
        collection.remove(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeAll_throws_exception() {
        collection.removeAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_throws_exception() {
        collection.contains(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_all_throws_exception() {
        collection.containsAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_retain_all_throws_exception() {
        collection.retainAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear_throws_exception() {
        collection.clear();
    }
}
