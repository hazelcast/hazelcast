package com.hazelcast.util.collection;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestJavaSerializationUtils;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InflatableSetTest {

    @Test(expected = IllegalArgumentException.class)
    public void whenInitialCapacityNegative_thenThrowIllegalArgumentException() {
        InflatableSet.newBuilder(-1);
    }

    @Test
    public void serialization_whenInInitialLoadingAndEmpty() throws IOException, ClassNotFoundException {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void serialization_whenInClosedState() throws IOException, ClassNotFoundException {
        Serializable object = TestJavaSerializationUtils.newSerializableObject(1);
        InflatableSet<Object> set = InflatableSet.newBuilder(1).add(object).build();
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void serialization_whenInflated() throws IOException, ClassNotFoundException {
        InflatableSet<Object> set = InflatableSet.newBuilder(0).build();
        set.add(TestJavaSerializationUtils.newSerializableObject(1));
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void clone_whenInflatedAndEntryInserted_thenCloneDoesNotContainTheObject() throws CloneNotSupportedException {
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
