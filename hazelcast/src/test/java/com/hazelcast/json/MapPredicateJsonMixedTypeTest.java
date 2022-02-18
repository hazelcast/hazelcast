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

package com.hazelcast.json;


import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPredicateJsonMixedTypeTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.OBJECT},
                {InMemoryFormat.BINARY}
        });
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    TestHazelcastInstanceFactory factory;
    HazelcastInstance instance;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(3);
        factory.newInstances(getConfig(), 3);
        instance = factory.getAllHazelcastInstances().iterator().next();
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig("default").setInMemoryFormat(inMemoryFormat);
        config.getSerializationConfig().addPortableFactory(1, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                if (classId == 1) {
                    return new Person();
                }
                return null;
            };
        });
        return config;
    }

    private HazelcastJsonValue createNameAgeOnDuty(String name, int age, boolean onDuty) {
        JsonObject object = Json.object();
        object.add("name", name);
        object.add("age", age);
        object.add("onDuty", onDuty);
        return new HazelcastJsonValue(object.toString());
    }

    @Test
    public void testPutGet() {
        IMap map = instance.getMap(randomMapName());

        map.put("k_int", 5);
        map.put("k_json", new HazelcastJsonValue("10"));
        map.put("k_int_2", 11);

        assertEquals(5, map.get("k_int"));
        assertEquals(10, Json.parse(((HazelcastJsonValue) map.get("k_json")).toString()).asInt());
        assertEquals(11, map.get("k_int_2"));
    }

    @Test
    public void testThisPredicate() {
        IMap map = instance.getMap(randomMapName());

        map.put("k_int", 5);
        map.put("k_json", new HazelcastJsonValue("10"));
        map.put("k_int_2", 11);

        Collection vals = map.values(Predicates.greaterEqual("this", 8));
        assertEquals(2, vals.size());
        assertContains(vals, new HazelcastJsonValue("10"));
        assertContains(vals, 11);
    }

    @Test
    public void testPortableWithSamePath() {
        IMap map = instance.getMap(randomMapName());

        map.put("k_1", new Person("a", 15, false));
        map.put("k_2", new Person("b", 27, true));
        map.put("k_3", createNameAgeOnDuty("c", 4, false));
        map.put("k_4", createNameAgeOnDuty("d", 77, false));

        Collection vals = map.keySet(Predicates.greaterEqual("age", 20));
        assertEquals(2, vals.size());
        assertContains(vals, "k_2");
        assertContains(vals, "k_4");
    }

    public static class Person implements Portable {

        private String name;
        private int age;
        private boolean onDuty;

        public Person() {
        }

        public Person(String name, int age, boolean onDuty) {
            this.name = name;
            this.age = age;
            this.onDuty = onDuty;
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
            writer.writeString("name", this.name);
            writer.writeInt("age", this.age);
            writer.writeBoolean("onDuty", this.onDuty);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.name = reader.readString("name");
            this.age = reader.readInt("age");
            this.onDuty = reader.readBoolean("onDuty");

        }
    }
}
