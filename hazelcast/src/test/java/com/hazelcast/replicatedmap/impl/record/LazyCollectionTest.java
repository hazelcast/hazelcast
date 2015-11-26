package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyObject;
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
        collection.add(anyObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_all_throws_exception() throws Exception {
        collection.addAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove_throws_exception() throws Exception {
        collection.remove(anyObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeAll_throws_exception() throws Exception {
        collection.removeAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_throws_exception() throws Exception {
        collection.contains(anyObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_all_throws_exception() throws Exception {
        collection.containsAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_retain_all_throws_exception() throws Exception {
        collection.retainAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear_throws_exception() throws Exception {
        collection.clear();
    }

}
