package com.hazelcast.query.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DefaultArgumentParserTest {

    private final DefaultArgumentParser parser = new DefaultArgumentParser();

    @Test
    public void passThrough_correctArgument() {
        // WHEN
        Object arguments = parser.parse("123");

        // THEN
        assertThat((String) arguments, equalTo("123"));
    }

    @Test
    public void passThrough_null() {
        // WHEN
        Object arguments = parser.parse(null);

        // THEN
        assertThat(arguments, equalTo(null));
    }

}
