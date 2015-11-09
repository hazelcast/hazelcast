package com.hazelcast.query.impl.getters;

import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractorGetterTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void isCacheable() {
        // GIVEN
        ExtractorGetter getter = new ExtractorGetter(mock(ValueExtractor.class), "argument");

        // THEN
        assertThat(getter.isCacheable(), is(true));
    }

    @Test
    public void getReturnType() {
        // GIVEN
        ExtractorGetter getter = new ExtractorGetter(mock(ValueExtractor.class), "argument");

        // EXPECT
        expected.expect(UnsupportedOperationException.class);

        // WHEN
        getter.getReturnType();
    }

}
