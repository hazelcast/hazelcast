package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
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

import static com.hazelcast.test.TestCollectionUtils.setOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapKeySetTest extends HazelcastTestSupport {

    private IMap<String, String> map;
    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance instance = createHazelcastInstance();
        
        map = instance.getMap(randomName());
        serializationService = getSerializationService(instance);
    }

    @Test(expected = NullPointerException.class)
    public void whenPredicateNull() {
        map.entrySet(null);
    }

    @Test
    public void whenMapEmpty() {
        Set<String> result = map.keySet(TruePredicate.INSTANCE);

        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<String> result = map.keySet();

        assertEquals(setOf("1", "2", "3"), result);
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Set<String> result = map.keySet(TruePredicate.INSTANCE);

        assertEquals(setOf("1", "2", "3"), result);
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Set<String> result = map.keySet(new GoodPredicate());

        assertEquals(setOf("1", "3"), result);
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Set<String> entries = map.keySet(TruePredicate.INSTANCE);

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, entries);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        assertEquals(serializationService.toData("1"), row.getKey());
        assertNull(row.getValue());
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }
    }
}
