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
public class LazySetTest {

    LazySet set;

    @Before
    public void setUp() throws Exception {
        KeySetIteratorFactory keySetIteratorFactory = mock(KeySetIteratorFactory.class);
        InternalReplicatedMapStorage storage = mock(InternalReplicatedMapStorage.class);
        set = new LazySet(keySetIteratorFactory, storage);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_throws_exception() throws Exception {
        set.add(anyObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_add_all_throws_exception() throws Exception {
        set.addAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_remove_throws_exception() throws Exception {
        set.remove(anyObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_removeAll_throws_exception() throws Exception {
        set.removeAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_throws_exception() throws Exception {
        set.contains(anyObject());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_contains_all_throws_exception() throws Exception {
        set.containsAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_retain_all_throws_exception() throws Exception {
        set.retainAll(anyCollection());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_clear_throws_exception() throws Exception {
        set.clear();
    }

}
