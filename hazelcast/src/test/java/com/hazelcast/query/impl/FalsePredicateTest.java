package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class FalsePredicateTest extends HazelcastTestSupport {

    private FalsePredicate falsePredicate;
    private SerializationService serializationService;

    @Before
    public void setup() {
        falsePredicate = new FalsePredicate();
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void apply() {
        Map.Entry entry = mock(Map.Entry.class);
        boolean result = falsePredicate.apply(entry);
        assertFalse(result);
    }

    @Test
    public void isIndexed() {
        QueryContext queryContext = mock(QueryContext.class);

        assertTrue(falsePredicate.isIndexed(queryContext));
    }

    @Test
    public void filter() {
        QueryContext queryContext = mock(QueryContext.class);

        Set<QueryableEntry> result = falsePredicate.filter(queryContext);

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void serialize() {
        Data data = serializationService.toData(falsePredicate);
        Object result = serializationService.toObject(data);
        assertInstanceOf(FalsePredicate.class, result);
    }

    @Test
    public void testToString() {
        String result = falsePredicate.toString();
        assertEquals("FalsePredicate{}", result);
    }
}
