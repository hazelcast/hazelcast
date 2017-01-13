package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiResultTest extends HazelcastTestSupport {

    private MultiResult<Object> result = new MultiResult<Object>();

    @Test
    public void addNull() {
        result.add(null);

        assertEquals(1, result.getResults().size());
        assertContains(result.getResults(), null);
    }

    @Test
    public void addValue() {
        result.add("007");

        assertEquals(1, result.getResults().size());
        assertContains(result.getResults(), "007");
    }

    @Test
    public void empty() {
        assertTrue(result.isEmpty());
    }

    @Test
    public void nonEmpty() {
        result.add("007");

        assertFalse(result.isEmpty());
    }

    @Test
    public void noLitter() {
        List<String> strings = asList("James", "Bond", "007");

        MultiResult<String> result = new MultiResult<String>(strings);

        assertSame(strings, result.getResults());
    }
}
