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
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InflatableSetTest {

    private InflatableSet<Object> set;

    @Before
    public void setUp() {
        set = new InflatableSet<Object>(10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenInitialLoadNegative_thenThrowIllegalArgumentException() {
        new InflatableSet<Object>(-1);
    }

    @Test
    public void serialization_whenInInitialLoadingAndEmpty() throws IOException, ClassNotFoundException {
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void serialization_whenInClosedState() throws IOException, ClassNotFoundException {
        set.add(TestJavaSerializationUtils.newSerializableObject(1));
        set.close();
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void serialization_whenInflated() throws IOException, ClassNotFoundException {
        set.close();
        set.add(TestJavaSerializationUtils.newSerializableObject(1));
        InflatableSet<Object> clone = TestJavaSerializationUtils.serializeAndDeserialize(set);
        assertEquals(set, clone);
    }

    @Test
    public void clone_whenInInitialLoadAndEntryInserted_thenCloneDoesNotContainTheObject() throws CloneNotSupportedException {
        InflatableSet<Object> clone = (InflatableSet<Object>) set.clone();
        set.add(new Object());

        assertThat(clone, is(empty()));
    }

    @Test
    public void clone_whenInflatedAndEntryInserted_thenCloneDoesNotContainTheObject() throws CloneNotSupportedException {
        set.close();
        set.add(new Object()); //inflate it
        InflatableSet<Object> clone = (InflatableSet<Object>) set.clone();
        set.add(new Object());

        assertThat(clone, hasSize(1));
    }


    @Test
    public void add_whenInInitialLoad_thenDoNotCallEquals() {
        MyObject o = new MyObject();
        set.add(o);
        assertThat(o.equalsCount, is(0));
    }

    @Test
    public void add_whenInInitialLoad_thenDoNotCallHashCode() {
        MyObject o = new MyObject();
        set.add(o);
        assertThat(o.hashCodeCount, is(0));
    }

    @Test
    public void add_whenClosed_thenDetectDuplicates() {
        MyObject o = new MyObject();
        set.add(o);
        set.close();
        boolean added = set.add(o);
        assertFalse(added);
    }

    @Test
    public void clear_whenInInitialLoad_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        set.add(o1);
        set.add(o2);
        set.clear();

        assertThat(set, is(empty()));
    }

    @Test
    public void clear_whenClosed_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        set.add(o1);
        set.add(o2);
        set.close();
        set.clear();

        assertThat(set, is(empty()));
    }

    @Test
    public void clear_whenInClosedModeAndAfterInsertion_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        set.add(o1);
        set.close();
        set.add(o2);
        set.clear();

        assertThat(set, is(empty()));
    }

    @Test
    public void clear_whenInClosedModeAndAfterLookUp_thenRemoveAllEntries() {
        MyObject o1 = new MyObject();
        MyObject o2 = new MyObject();

        set.add(o1);
        set.add(o2);
        set.close();
        set.contains(o1);
        set.clear();

        assertThat(set, is(empty()));
    }


    @Test(expected = IllegalStateException.class)
    public void close_whenNotInInitialLoad_thenThrowIllegalStateException() {
        set.close();
        set.close();
    }

    @Test
    public void remove_whenInInitialLoad_thenRemoveObject() {
        MyObject o = new MyObject();
        set.add(o);
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void remove_whenClosed_thenRemoveObject() {
        MyObject o = new MyObject();
        set.add(o);
        set.close();
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void remove_whenClosedAndAfterLookup_thenRemoveObject() {
        MyObject o = new MyObject();
        set.add(o);
        set.close();
        set.contains(o);
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void remove_whenClosedAndAfterInsertion_thenRemoveObject() {
        set.close();
        MyObject o = new MyObject();
        set.add(o);
        set.remove(o);

        assertThat(set, is(empty()));
    }

    @Test
    public void size_whenInInitalState_thenReturnSize() {
        MyObject o = new MyObject();
        set.add(o);

        assertThat(set, hasSize(1));
    }

    @Test
    public void size_whenClosedAndAfterInsertion_thenReturnSize() {
        set.close();
        MyObject o = new MyObject();
        set.add(o);

        assertThat(set, hasSize(1));
    }

    @Test
    public void contains_whenInInitialLoadAndObjectInserted_thenReturnTrue() {
        MyObject o = new MyObject();
        set.add(o);

        assertThat(set.contains(o), is(true));
    }


    @Test(expected = ConcurrentModificationException.class)
    public void iterator_next_whenModifiedInClosedState_thenFailFast() {
        MyObject o1 = new MyObject();
        set.add(o1);
        set.close();

        Iterator<Object> iterator = set.iterator();
        MyObject o2 = new MyObject();
        set.add(o2);

        iterator.next();
    }

    @Test
    public void iterator_remove_whenClosedAndLookedUp_thenRemoveFromCollection() {
        MyObject o1 = new MyObject();
        set.add(o1);
        set.close();

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
