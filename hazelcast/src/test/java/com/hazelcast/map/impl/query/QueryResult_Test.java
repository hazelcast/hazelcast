package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryResult_Test extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        serializationService = getSerializationService(hz);
    }

    @Test
    public void isEmpty_whenNotEmpty() {
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, 100);

        boolean result = queryResult.isEmpty();

        assertTrue(result);
    }

    @Test
    public void isEmpty_whenEmpty() {
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, 100);
        queryResult.add(toData("1"),toData("1"));

        boolean result = queryResult.isEmpty();

        assertFalse(result);
    }

    @Test
    public void limit() {
        QueryResult queryResult = new QueryResult(IterationType.KEY, 2);
        queryResult.addFrom(new QueryResultRow(toData(1), null));
        queryResult.addFrom(new QueryResultRow(toData(2), null));

        try {
            queryResult.addFrom(new QueryResultRow(toData(3), null));
            fail();
        } catch (QueryResultSizeExceededException e) {

        }
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }

    @Test
    public void test() {
        QueryResult queryResult = new QueryResult(IterationType.KEY, Long.MAX_VALUE);

        int itemCount = 1000;

        Data[] keys = new Data[itemCount];
        for (int k = 0; k < itemCount; k++) {
            Data key = serializationService.toData(k);
            keys[k] = key;
            queryResult.addFrom(new QueryResultRow(key, null));
        }

        QueryResult.Cursor cursor = queryResult.openCursor();
        for (int k = 0; k < 1000; k++) {
            assertTrue(cursor.next());
            assertEquals(keys[k], cursor.getKey());
            assertNull(cursor.getValue());
        }

        assertFalse(cursor.next());
    }
}
