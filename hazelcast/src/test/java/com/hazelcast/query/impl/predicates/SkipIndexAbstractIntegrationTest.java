package com.hazelcast.query.impl.predicates;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;


public abstract class SkipIndexAbstractIntegrationTest extends HazelcastTestSupport {

    @Test
    public void testIndexSuppressionDoesntChangeOutcome() {
        HazelcastInstance hz = getHazelcastInstance();
        IMap<Integer, Pojo> map = hz.getMap("foo");
        map.addIndex("f", false);

        for (int k = 0; k < 20; k++) {
            map.put(k, new Pojo(k));
        }

        assertEquals(1, map.values(new SqlPredicate("%f=10")).size());
        assertEquals(9, map.values(new SqlPredicate("%f>10")).size());
        assertEquals(10, map.values(new SqlPredicate("%f>=10")).size());
        assertEquals(19, map.values(new SqlPredicate("%f!=10")).size());
        assertEquals(10, map.values(new SqlPredicate("%f<10")).size());
        assertEquals(11, map.values(new SqlPredicate("%f<=10")).size());
        assertEquals(5, map.values(new SqlPredicate("%f in (1,2,3,4,5)")).size());

        hz.shutdown();
    }

    public abstract HazelcastInstance getHazelcastInstance();

    public static class Pojo implements Serializable {

        public int f;

        public Pojo(int f) {
            this.f = f;
        }

    }
}
