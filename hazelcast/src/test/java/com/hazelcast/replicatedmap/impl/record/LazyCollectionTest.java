package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LazyCollectionTest {

    LazyCollection collection;

    @Before
    public void setUp() throws Exception {
        ValuesIteratorFactory iteratorFactory = mock(ValuesIteratorFactory.class);
        InternalReplicatedMapStorage storage = mock(InternalReplicatedMapStorage.class);
        collection = new LazyCollection(iteratorFactory, storage);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_throws_exception() throws Exception {
        collection.add(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_all_throws_exception() throws Exception {
        collection.addAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove_throws_exception() throws Exception {
        collection.remove(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeAll_throws_exception() throws Exception {
        collection.removeAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_throws_exception() throws Exception {
        collection.contains(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_all_throws_exception() throws Exception {
        collection.containsAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_retain_all_throws_exception() throws Exception {
        collection.retainAll(Collections.EMPTY_LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear_throws_exception() throws Exception {
        collection.clear();
    }

}
