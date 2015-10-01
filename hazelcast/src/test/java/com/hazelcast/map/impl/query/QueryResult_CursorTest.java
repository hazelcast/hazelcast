package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.SerializationService;
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
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryResult_CursorTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        serializationService = getSerializationService(hz);
    }

    @Test
    public void testWhenEmpty() {
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, Long.MAX_VALUE);

        QueryResult.Cursor cursor = queryResult.openCursor();

        assertFalse(cursor.hasNext());
        assertFalse(cursor.next());
    }

    @Test
    public void testSingleItem() {
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, Long.MAX_VALUE);
        Data key = toData("1");
        Data value = toData("a");
        queryResult.add(key, value);

        QueryResult.Cursor cursor = queryResult.openCursor();
        assertTrue(cursor.hasNext());
        assertTrue(cursor.next());

        assertEquals(key, cursor.getKey());
        assertEquals(value, cursor.getValue());

        assertFalse(cursor.hasNext());
        assertFalse(cursor.next());
    }

    @Test
    public void testManyItems() {
        QueryResult queryResult = new QueryResult(IterationType.ENTRY, Long.MAX_VALUE);

        int count = 100;

        Data[] keys = new Data[count];
        Data[] values = new Data[count];
        for (int k = 0; k < count; k++) {
            keys[k] = toData("key-" + k);
            values[k] = toData("value-" + k);
            queryResult.add(keys[k], values[k]);
        }


        QueryResult.Cursor cursor = queryResult.openCursor();
        for (int k = 0; k < count; k++) {
            assertTrue(cursor.hasNext());
            assertTrue(cursor.next());

            assertEquals(keys[k], cursor.getKey());
            assertEquals(values[k], cursor.getValue());
        }

        assertFalse(cursor.hasNext());
        assertFalse(cursor.next());
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }

}
