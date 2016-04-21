package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.InstanceOfPredicate;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapValuesTest extends HazelcastTestSupport {

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
        map.values(null);
    }

    @Test
    public void whenMapEmpty() {
        Collection<String> result = map.values(TruePredicate.INSTANCE);

        assertTrue(result.isEmpty());
    }

    @Test
    public void whenSelecting_withoutPredicate() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Collection<String> result = map.values();

        assertEquals(3, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test
    public void whenSelectingAllEntries() {
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");

        Collection<String> result = map.values(TruePredicate.INSTANCE);

        assertEquals(3, result.size());
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test
    public void whenSelectingSomeEntries() {
        map.put("1", "good1");
        map.put("2", "bad");
        map.put("3", "good2");

        Collection<String> result = map.values(new GoodPredicate());

        assertEquals(2, result.size());
        assertTrue(result.contains("good1"));
        assertTrue(result.contains("good2"));
    }

    @Test
    public void testResultType() {
        map.put("1", "a");
        Collection<String> entries = map.values(TruePredicate.INSTANCE);

        QueryResultCollection collection = assertInstanceOf(QueryResultCollection.class, entries);
        QueryResultRow row = (QueryResultRow) collection.getRows().iterator().next();
        // there should only be a value; no key.
        assertNull(row.getKey());
        assertEquals(serializationService.toData("a"), row.getValue());
    }

    @Test
    public void testSerializationServiceNullClassLoaderProblem() throws Exception {
        // If the classloader is null the following call throws NullPointerException
        map.values(new InstanceOfPredicate(SampleObjects.PortableEmployee.class));
    }

    static class GoodPredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return mapEntry.getValue().startsWith("good");
        }
    }
}
