package com.hazelcast.map.impl.query;

import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.operation.AbstractMapOperation;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueryPartitionOperationTest extends QueryOperationTestSupport {

    @Override
    protected AbstractMapOperation createQueryOperation() {
        return new QueryPartitionOperation(MAP_NAME, TruePredicate.INSTANCE, IterationType.ENTRY);
    }

    @Test
    public void testConstructor() {
        new QueryPartitionOperation();
    }

    @Test
    public void testRun_resultSizeLimitOff() throws Exception {
        initMocks(Long.MAX_VALUE, 5);

        QueryResult result = getQueryResult();
        assertEquals(5, result.getResult().size());
    }

    @Test
    public void testRun_resultSizeLimit_notExceeded() throws Exception {
        initMocks(3, 2);

        QueryResult result = getQueryResult();
        assertEquals(2, result.getResult().size());
    }

    @Test
    public void testRun_resultSizeLimit_equals() throws Exception {
        initMocks(3, 3);

        QueryResult result = getQueryResult();
        assertEquals(3, result.getResult().size());
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testRun_resultSizeLimit_exceeded() throws Exception {
        initMocks(3, 4);

        getQueryResult();
    }
}
