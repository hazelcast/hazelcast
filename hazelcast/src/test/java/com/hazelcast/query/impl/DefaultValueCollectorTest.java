package com.hazelcast.query.impl;

import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DefaultValueCollectorTest {

    private DefaultValueCollector collector;

    @Before
    public void setup() {
        collector = new DefaultValueCollector();
    }

    @Test
    public void test_emptyCollector() {
        assertNull(collector.getResult());
    }

    @Test
    public void test_singleObject() {
        collector.addObject(1);

        assertEquals(1, collector.getResult());
    }

    @Test
    public void test_TwoObjects() {
        collector.addObject(1);
        collector.addObject(2);

        List<Integer> results = assertIsMultiResultAndGetResults(collector.getResult());
        assertThat(results, hasSize(2));
        assertThat(results, containsInAnyOrder(1, 2));
    }

    @Test
    public void test_multipleObjects() {
        collector.addObject(1);
        collector.addObject(2);
        collector.addObject(3);

        List<Integer> results = assertIsMultiResultAndGetResults(collector.getResult());
        assertThat(results, hasSize(3));
        assertThat(results, containsInAnyOrder(1, 2, 3));
    }

    @Test
    public void test_multipleObjects_sameValues() {
        collector.addObject(1);
        collector.addObject(1);

        List<Integer> results = assertIsMultiResultAndGetResults(collector.getResult());
        assertThat(results, hasSize(2));
        assertThat(results, containsInAnyOrder(1, 1));
    }

    @Test
    public void test_multipleObjects_includingNull() {
        collector.addObject(1);
        collector.addObject(null);

        List<Integer> results = assertIsMultiResultAndGetResults(collector.getResult());
        assertThat(results, hasSize(2));
        assertThat(results, containsInAnyOrder(1, null));
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> assertIsMultiResultAndGetResults(Object result) {
        assertTrue(result instanceof MultiResult);
        return ((MultiResult<T>) collector.getResult()).getResults();
    }
}
