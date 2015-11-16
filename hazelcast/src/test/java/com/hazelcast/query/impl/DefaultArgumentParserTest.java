package com.hazelcast.query.impl;

import com.hazelcast.query.extractor.Arguments;
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

    private final DefaultArgumentsParser parser = new DefaultArgumentsParser();

    @Test
    public void passThrough_correctArgument() {
        // WHEN
        Arguments arguments = parser.parse("123");

        // THEN
        assertThat((String) arguments.get(), equalTo("123"));
    }

    @Test
    public void passThrough_null() {
        // WHEN
        Arguments arguments = parser.parse(null);

        // THEN
        assertThat(arguments.get(), equalTo(null));
    }

}
