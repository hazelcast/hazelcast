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
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class LazySetTest {

    private LazySet<Object, Object, Object> set;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        KeySetIteratorFactory<Object, Object> keySetIteratorFactory = mock(KeySetIteratorFactory.class);
        InternalReplicatedMapStorage<Object, Object> storage = mock(InternalReplicatedMapStorage.class);
        set = new LazySet<Object, Object, Object>(keySetIteratorFactory, storage);
    }

    @Test(expected = UnsupportedOperationException.class)
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void testContains_throwsException() {
        set.contains(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContainsAll_throwsException() {
        set.containsAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAdd_throwsException() {
        set.add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddAll_throwsException() {
        set.addAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove_throwsException() {
        set.remove(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAll_throwsException() {
        set.removeAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetainAll_throwsException() {
        set.retainAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClear_throwsException() {
        set.clear();
    }
}
