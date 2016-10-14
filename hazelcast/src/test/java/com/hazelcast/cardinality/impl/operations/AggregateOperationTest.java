package com.hazelcast.cardinality.impl.operations;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AggregateOperationTest {

    private AggregateOperation operation;

    @Before
    public void setUp() throws Exception {
        operation = new AggregateOperation("testName", 1984127);
    }

    @Test
    public void testName() {
        String operationString = operation.toString();
        assertNotNull(operationString);
        assertTrue(operationString.contains("AggregateOperation"));
        assertTrue(operationString.contains("name=testName"));
    }
}
