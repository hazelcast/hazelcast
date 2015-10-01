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
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryResult_SerializationTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        serializationService = getSerializationService(hz);
    }

    @Test
    public void whenIterationTypeEntry() {
        QueryResult expected = new QueryResult(IterationType.ENTRY, 100);
        QueryResultRow row = new QueryResultRow(toData("key"), toData("valu"));
        expected.addFrom(row);

        QueryResult actual = clone(expected);

        assertNotNull(actual);
        assertEquals(expected.getIterationType(), actual.getIterationType());
        assertEquals(1, actual.size());
        assertEquals(expected.getRows(), actual.getRows());
    }

    @Test
    public void whenIterationTypeValue() {
        QueryResult expected = new QueryResult(IterationType.VALUE, 100);
        QueryResultRow row = new QueryResultRow(null, toData("value"));
        expected.addFrom(row);

        QueryResult actual = clone(expected);

        assertNotNull(actual);
        assertEquals(expected.getIterationType(), actual.getIterationType());
        assertEquals(1, actual.size());
        assertEquals(expected.getRows(), actual.getRows());
    }

    @Test
    public void whenIterationTypeKey() {
        QueryResult expected = new QueryResult(IterationType.KEY, 100);
        QueryResultRow row = new QueryResultRow(toData("key"), null);
        expected.addFrom(row);

        QueryResult actual = clone(expected);

        assertNotNull(actual);
        assertEquals(expected.getIterationType(), actual.getIterationType());
        assertEquals(1, actual.size());
        assertEquals(expected.getRows(), actual.getRows());
    }

    private QueryResult clone(QueryResult result) {
        Data data = serializationService.toData(result);
        return serializationService.toObject(data);
    }

    private Data toData(Object item) {
        return serializationService.toData(item);
    }
}
