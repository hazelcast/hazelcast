package com.hazelcast.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class PredicateBuilderTest extends HazelcastTestSupport {

    @Test
    public void get_keyAttribute() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.key().get("id").equal("10");

        IMap<Id, Integer> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(new Id("10"), 1);
        hazelcastLookupMap.put(new Id("20"), 2);
        hazelcastLookupMap.put(new Id("30"), 3);

        Collection<Integer> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(1));
    }

    @Test
    public void get_key() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.key().equal(10L);

        IMap<Integer, Integer> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(10, 1);
        hazelcastLookupMap.put(30, 2);

        Collection<Integer> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(1));
    }

    @Test
    public void get_this() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.get("this").equal(1L);

        IMap<Integer, Integer> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(10, 1);
        hazelcastLookupMap.put(30, 2);

        Collection<Integer> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(1));
    }

    @Test
    public void get_attribute() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance hz = factory.newHazelcastInstance();

        EntryObject e = new PredicateBuilder().getEntryObject();

        Predicate predicate = e.get("id").equal("10");

        IMap<Integer, Id> hazelcastLookupMap = hz.getMap("somemap");

        hazelcastLookupMap.put(1,new Id("10"));
        hazelcastLookupMap.put(2,new Id("20"));
        hazelcastLookupMap.put(3,new Id("30"));

        Collection<Id> result = hazelcastLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertTrue(result.contains(new Id("10")));
    }

    public static class Id implements Serializable {
        private String id;

        public Id(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Id id1 = (Id) o;

            if (id != null ? !id.equals(id1.id) : id1.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }
}
