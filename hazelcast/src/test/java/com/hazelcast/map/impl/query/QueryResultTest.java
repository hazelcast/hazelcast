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
public class QueryResultTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        serializationService = getSerializationService(hz);
    }

    @Test
    public void serialization() {
        QueryResult expected = new QueryResult(IterationType.ENTRY, 100);
        QueryResultRow row = new QueryResultRow(serializationService.toData("1"), serializationService.toData("row"));
        expected.addRow(row);

        QueryResult actual = clone(expected);

        assertNotNull(actual);
        assertEquals(expected.getIterationType(), actual.getIterationType());
        assertEquals(1, actual.size());
        assertEquals(row, actual.iterator().next());
    }

    private QueryResult clone(QueryResult result) {
        Data data = serializationService.toData(result);
        return serializationService.toObject(data);
    }
}
