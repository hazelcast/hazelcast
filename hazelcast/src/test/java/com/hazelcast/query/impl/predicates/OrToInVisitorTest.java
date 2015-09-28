package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.or;
import static com.hazelcast.query.impl.TypeConverters.INTEGER_CONVERTER;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OrToInVisitorTest {

    private OrToInVisitor visitor;
    private Indexes mockIndexes;
    private Index mockIndex;

    @Before
    public void setUp() {
        mockIndexes = mock(Indexes.class);
        mockIndex = mock(Index.class);
        when(mockIndexes.getIndex(anyString())).thenReturn(mockIndex);
        visitor = new OrToInVisitor();
        useConverter(INTEGER_CONVERTER);
    }

    @Test
    public void whenThresholdExceeded_thenRewriteToInPredicate() {
        //(age = 1 or age = 2 or age = 3 or age = 4 or age = 5)  -->  (age in (1, 2, 3, 4, 5))
        Predicate p1 = equal("age", 1);
        Predicate p2 = equal("age", 2);
        Predicate p3 = equal("age", 3);
        Predicate p4 = equal("age", 4);
        Predicate p5 = equal("age", 5);
        OrPredicate or = (OrPredicate) or(p1, p2, p3, p4, p5);
        InPredicate result = (InPredicate) visitor.visit(or, mockIndexes);
        Comparable[] values = result.values;
        assertThat(values, arrayWithSize(5));
        assertThat(values, Matchers.is(Matchers.<Comparable>arrayContainingInAnyOrder(1, 2, 3, 4, 5)));

    }



    private void useConverter(TypeConverter converter) {
        when(mockIndex.getConverter()).thenReturn(converter);
    }
}
