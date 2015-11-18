package com.hazelcast.query.impl.predicates;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.extractor.Arguments;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NestedPredicateWithExtractorTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private IMap<Integer, Body> map;

    private static int bodyExtractorExecutions;
    private static int limbExtractorExecutions;

    public static class BodyNameExtractor extends ValueExtractor<Body, Object> {
        @Override
        public void extract(Body target, Arguments arguments, ValueCollector collector) {
            bodyExtractorExecutions++;
            collector.addObject(target.getName());
        }
    }

    public static class LimbNameExtractor extends ValueExtractor<Body, Object> {
        @Override
        public void extract(Body target, Arguments arguments, ValueCollector collector) {
            limbExtractorExecutions++;
            collector.addObject(target.getLimb().getName());
        }
    }

    @Before
    public void setup() {
        bodyExtractorExecutions = 0;
        limbExtractorExecutions = 0;
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        mapConfig.addMapAttributeConfig(extractor("name", "com.hazelcast.query.impl.predicates.NestedPredicateWithExtractorTest$BodyNameExtractor"));
        mapConfig.addMapAttributeConfig(extractor("limbname", "com.hazelcast.query.impl.predicates.NestedPredicateWithExtractorTest$LimbNameExtractor"));
        config.addMapConfig(mapConfig);
        instance = createHazelcastInstance(config);
        map = instance.getMap("map");
    }

    static MapAttributeConfig extractor(String name, String extractor) {
        MapAttributeConfig extractorConfig = new MapAttributeConfig();
        extractorConfig.setName(name);
        extractorConfig.setExtractor(extractor);
        return extractorConfig;
    }

    @Test
    public void singleAttributeQuery_predicates() throws Exception {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("name").equal("body1");
        Collection<Body> values = map.values(predicate);

        // THEN
        assertEquals(1, values.size());
        assertEquals("body1", values.toArray(new Body[]{})[0].getName());
        assertEquals(2 + 1, bodyExtractorExecutions);
        assertEquals(0, limbExtractorExecutions);
    }

    @Test
    public void singleAttributeQuery_distributedSql() throws Exception {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        Collection<Body> values = map.values(new SqlPredicate("name == 'body1'"));

        // THEN
        assertEquals(1, values.size());
        assertEquals("body1", values.toArray(new Body[]{})[0].getName());
        assertEquals(2 + 1, bodyExtractorExecutions);
        assertEquals(0, limbExtractorExecutions);
    }

    @Test
    public void nestedAttributeQuery_predicates() throws Exception {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("limbname").equal("leg");
        Collection<Body> values = map.values(predicate);

        // THEN
        assertEquals(1, values.size());
        assertEquals("body2", values.toArray(new Body[]{})[0].getName());
        assertEquals(0, bodyExtractorExecutions);
        assertEquals(2 + 1, limbExtractorExecutions);
    }

    @Test
    public void nestedAttributeQuery_distributedSql() throws Exception {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        Collection<Body> values = map.values(new SqlPredicate("limbname == 'leg'"));

        // THEN
        assertEquals(1, values.size());
        assertEquals("body2", values.toArray(new Body[]{})[0].getName());
        assertEquals(0, bodyExtractorExecutions);
        assertEquals(2 + 1, limbExtractorExecutions);
    }


    private static class Body implements Serializable {
        private final String name;
        private final Limb limb;

        public Body(String name, Limb limb) {
            this.name = name;
            this.limb = limb;
        }

        public String getName() {
            return name;
        }

        public Limb getLimb() {
            return limb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Body body = (Body) o;

            if (name != null ? !name.equals(body.name) : body.name != null) return false;
            return !(limb != null ? !limb.equals(body.limb) : body.limb != null);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (limb != null ? limb.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Body{" +
                    "name='" + name + '\'' +
                    ", limb=" + limb +
                    '}';
        }
    }

    private static class Limb implements Serializable {
        private final String name;

        public Limb(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Limb limb = (Limb) o;

            return !(name != null ? !name.equals(limb.name) : limb.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Limb{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

}
