package com.hazelcast.query.impl.getters;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiResultTest {

    private MultiResult<Object> result = new MultiResult<Object>();

    @Test
    public void addNull() {
        // WHEN
        result.add(null);

        // THEN
        assertThat(result.getResults().size(), is(1));
        assertTrue(result.getResults().contains(null));
    }

    @Test
    public void addValue() {
        // WHEN
        result.add("007");

        // THEN
        assertThat(result.getResults().size(), is(1));
        assertTrue(result.getResults().contains("007"));
    }

    @Test
    public void empty() {
        // THEN
        assertThat(result.isEmpty(), is(true));
    }

    @Test
    public void nonEmpty() {
        // WHEN
        result.add("007");

        // THEN
        assertThat(result.isEmpty(), is(false));
    }

    @Test
    public void noLitter() {
        // GIVEN
        List<String> strings = asList("James", "Bond", "007");

        // WHEN
        MultiResult<String> result = new MultiResult<String>(strings);

        // THEN
        assertThat(result.getResults(), is(strings));
    }

}
