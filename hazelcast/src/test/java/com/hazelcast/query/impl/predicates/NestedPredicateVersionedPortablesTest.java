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
import com.hazelcast.config.IndexType;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.nio.serialization.VersionedPortable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class NestedPredicateVersionedPortablesTest extends HazelcastTestSupport {

    private IMap<Integer, Body> map;

    @Before
    public void setup() {
        Config config = new Config();
        setUpFactory(config);
        HazelcastInstance instance = createHazelcastInstance(config);
        map = instance.getMap("map");
    }

    public SerializationConfig setUpFactory(Config config) {
        return config.getSerializationConfig().addPortableFactory(1, (classId) -> {
            switch (classId) {
                case 1:
                    return new Body();
                case 2:
                    return new Limb();
                default:
                    throw new IllegalStateException("Wrong class ID");
            }
        });
    }

    @After
    public void tearDown() {
        shutdownNodeFactory();
    }

    @Test
    public void addingIndexes() {
        // single-attribute index
        map.addIndex(IndexType.SORTED, "name");
        // nested-attribute index
        map.addIndex(IndexType.SORTED, "limb.name");
    }

    @Test
    public void singleAttributeQuery_versionedProtables_predicates() {
        // GIVEN
        map.put(1, new NestedPredicateVersionedPortablesTest.Body("body1", new NestedPredicateVersionedPortablesTest.Limb("hand")));
        map.put(2, new NestedPredicateVersionedPortablesTest.Body("body2", new NestedPredicateVersionedPortablesTest.Limb("leg")));

        // WHEN
        EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = e.get("limb.name").equal("hand");
        Collection<NestedPredicateVersionedPortablesTest.Body> values = map.values(predicate);

        // THEN
        assertEquals(1, values.size());
        assertEquals("body1", values.toArray(new Body[0])[0].getName());

    }

    @Test
    public void nestedAttributeQuery_distributedSql() {
        // GIVEN
        map.put(1, new NestedPredicateVersionedPortablesTest.Body("body1", new NestedPredicateVersionedPortablesTest.Limb("hand")));
        map.put(2, new NestedPredicateVersionedPortablesTest.Body("body2", new NestedPredicateVersionedPortablesTest.Limb("leg")));

        // WHEN
        Collection<NestedPredicateVersionedPortablesTest.Body> values = map.values(Predicates.sql("limb.name == 'leg'"));

        // THEN
        assertEquals(1, values.size());
        assertEquals("body2", values.toArray(new Body[0])[0].getName());
    }


    private static class Body implements VersionedPortable {

        private String name;
        private Limb limb;

        Body(String name, Limb limb) {
            this.name = name;
            this.limb = limb;
        }

        Body() {
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

        @Override
        public int getClassVersion() {
            return 15;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("name", name);
            writer.writePortable("limb", limb);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
            limb = reader.readPortable("limb");
        }
    }

    private static class Limb implements VersionedPortable {

        private String name;

        Limb(String name) {
            this.name = name;
        }

        Limb() {
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

        @Override
        public int getClassVersion() {
            return 2;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("name", name);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
        }
    }
}
