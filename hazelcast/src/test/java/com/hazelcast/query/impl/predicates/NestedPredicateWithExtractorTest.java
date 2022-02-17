/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.impl.predicates;

import com.hazelcast.config.Config;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NestedPredicateWithExtractorTest extends HazelcastTestSupport {

    private IMap<Integer, Body> map;

    private static int bodyExtractorExecutions;
    private static int limbExtractorExecutions;

    @Before
    public void setup() {
        bodyExtractorExecutions = 0;
        limbExtractorExecutions = 0;
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("map");
        mapConfig.addAttributeConfig(extractor("name",
                "com.hazelcast.query.impl.predicates.NestedPredicateWithExtractorTest$BodyNameExtractor"));
        mapConfig.addAttributeConfig(extractor("limbname",
                "com.hazelcast.query.impl.predicates.NestedPredicateWithExtractorTest$LimbNameExtractor"));
        config.addMapConfig(mapConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        map = instance.getMap("map");
    }

    private static AttributeConfig extractor(String name, String extractor) {
        AttributeConfig extractorConfig = new AttributeConfig();
        extractorConfig.setName(name);
        extractorConfig.setExtractorClassName(extractor);
        return extractorConfig;
    }

    @After
    public void tearDown() {
        shutdownNodeFactory();
    }

    @Test
    public void singleAttributeQuery_predicates() {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.get("name").equal("body1");
        Collection<Body> values = map.values(predicate);

        // THEN
        assertEquals(1, values.size());
        assertEquals("body1", values.toArray(new Body[0])[0].getName());
        assertEquals(2, bodyExtractorExecutions);
        assertEquals(0, limbExtractorExecutions);
    }

    @Test
    public void singleAttributeQuery_distributedSql() {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        Collection<Body> values = map.values(Predicates.sql("name == 'body1'"));

        // THEN
        assertEquals(1, values.size());
        assertEquals("body1", values.toArray(new Body[0])[0].getName());
        assertEquals(2, bodyExtractorExecutions);
        assertEquals(0, limbExtractorExecutions);
    }

    @Test
    public void nestedAttributeQuery_predicates() {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.get("limbname").equal("leg");
        Collection<Body> values = map.values(predicate);

        // THEN
        assertEquals(1, values.size());
        assertEquals("body2", values.toArray(new Body[0])[0].getName());
        assertEquals(0, bodyExtractorExecutions);
        assertEquals(2, limbExtractorExecutions);
    }

    @Test
    public void nestedAttributeQuery_customPredicates_entryProcessor() {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        Map<Integer, Object> result = map.executeOnEntries(new ExtractProcessor(), new CustomPredicate());
        Body resultBody = (Body) result.values().iterator().next();

        // THEN
        assertEquals(1, result.size());
        assertEquals("body1", resultBody.getName());
    }

    private static final class ExtractProcessor implements EntryProcessor<Integer, Body, Body> {
        @Override
        public Body process(Map.Entry<Integer, Body> entry) {
            return entry.getValue();
        }

        @Override
        public EntryProcessor<Integer, Body, Body> getBackupProcessor() {
            return null;
        }
    }

    private static final class CustomPredicate extends AbstractPredicate {

        CustomPredicate() {
            super("limbname");
        }

        @Override
        protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
            return attributeValue.equals("hand");
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    @Test
    public void nestedAttributeQuery_distributedSql() {
        // GIVEN
        map.put(1, new Body("body1", new Limb("hand")));
        map.put(2, new Body("body2", new Limb("leg")));

        // WHEN
        Collection<Body> values = map.values(Predicates.sql("limbname == 'leg'"));

        // THEN
        assertEquals(1, values.size());
        assertEquals("body2", values.toArray(new Body[0])[0].getName());
        assertEquals(0, bodyExtractorExecutions);
        assertEquals(2, limbExtractorExecutions);
    }

    public static class BodyNameExtractor implements ValueExtractor<Body, Object> {

        @Override
        public void extract(Body target, Object arguments, ValueCollector collector) {
            bodyExtractorExecutions++;
            collector.addObject(target.getName());
        }
    }

    public static class LimbNameExtractor implements ValueExtractor<Body, Object> {

        @Override
        public void extract(Body target, Object arguments, ValueCollector collector) {
            limbExtractorExecutions++;
            collector.addObject(target.getLimb().getName());
        }
    }

    private static class Body implements Serializable {

        private final String name;
        private final Limb limb;

        Body(String name, Limb limb) {
            this.name = name;
            this.limb = limb;
        }

        String getName() {
            return name;
        }

        Limb getLimb() {
            return limb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Body body = (Body) o;
            if (name != null ? !name.equals(body.name) : body.name != null) {
                return false;
            }
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
            return "Body{"
                    + "name='" + name + '\''
                    + ", limb=" + limb
                    + '}';
        }
    }

    private static class Limb implements Serializable {
        private final String name;

        Limb(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Limb limb = (Limb) o;
            return !(name != null ? !name.equals(limb.name) : limb.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "Limb{"
                    + "name='" + name + '\''
                    + '}';
        }
    }
}
