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

package com.hazelcast.internal.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestJavaSerializationUtils;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InflatableSetTest {

    @Test
    public void testBuilderSize() {
        InflatableSet.Builder<MyObject> builder = InflatableSet.newBuilder(1);

        builder.add(new MyObject());
        assertEquals(1, builder.size());

        builder.add(new MyObject());
        assertEquals(2, builder.size());

        InflatableSet<MyObject> set = builder.build();
        assertEquals(2, set.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenInitialCapacityNegative_thenThrowIllegalArgumentException() {
        InflatableSet.newBuilder(-1);
    }

    @Test(expected = NullPointerException.class)
    public void whenNullListPassed_thenNullPointerException() {
        InflatableSet.newBuilder(null);
    }

    @Test
    public void serialization_whenInInitialLoadingAndEmpty() throws Exception {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void serialization_whenInClosedState() throws Exception {
        Serializable object = TestJavaSerializationUtils.newSerializableObject(1);
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(object).build();
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void serialization_whenInflated() throws Exception {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        set.add(TestJavaSerializationUtils.newSerializableObject(1));
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void clone_whenInflatedAndEntryInserted_thenCloneDoesNotContainTheObject() {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        set.add(new Object()); //inflate it
        InflatableSet<Object> clone = (InflatableSet<Object>) set.clone();
        set.add(new Object());

        assertThat(clone, hasSize(1));
    }


    @Test
    public void add_whenClosed_thenDetectDuplicates() {
        MyObject o = new MyObject();
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(o).build();
        boolean added = set.add(o);
        assertFalse(added);
    }

    @Test
    public void clear_whenClosed_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        InflatableSet<Object> set = InflatableSet.newBuilder(2).add(o1).add(o2).build();
        set.clear();

        assertThat(set, is(empty()));
    }

    @Test
    public void clear_whenInClosedModeAndAfterInsertion_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(o1).build();
        set.add(o2);
        set.clear();

        assertThat(set, is(empty()));
    }

    @Test
    public void clear_whenInClosedModeAndAfterLookUp_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        InflatableSet<Object> set = InflatableSet.newBuilder(2).add(o1).add(o2).build();
        set.contains(o1);
        set.clear();

        assertThat(set, is(empty()));
    }

    @Test
    public void remove_whenClosed_thenRemoveObject() {
        MyObject o = new MyObject();
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(o).build();
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void remove_whenClosedAndAfterLookup_thenRemoveObject() {
        MyObject o = new MyObject();
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(o).build();
        set.contains(o);
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void remove_whenClosedAndAfterInsertion_thenRemoveObject() {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        MyObject o = new MyObject();
        set.add(o);
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void size_whenClosedAndAfterInsertion_thenReturnSize() {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        MyObject o = new MyObject();
        set.add(o);

        assertThat(set, hasSize(1));
    }

    @Test(expected = ConcurrentModificationException.class)
    public void iterator_next_whenModifiedInClosedState_thenFailFast() {
        MyObject o1 = new MyObject();
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(o1).build();

        Iterator<Object> iterator = set.iterator();
        MyObject o2 = new MyObject();
        set.add(o2);

        iterator.next();
    }

    @Test
    public void iterator_remove_whenClosedAndLookedUp_thenRemoveFromCollection() {
        MyObject o1 = new MyObject();
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(o1).build();

        Iterator<Object> iterator = set.iterator();
        set.contains(o1);
        iterator.next();
        iterator.remove();

        assertThat(set, is(empty()));
    }

    private static class MyObject {
        int equalsCount;
        int hashCodeCount;

        @Override
        public boolean equals(Object obj) {
            equalsCount++;
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            hashCodeCount++;
            return super.hashCode();
        }
    }
}
