/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPredicateJsonTest extends HazelcastTestSupport {

    TestHazelcastInstanceFactory factory;
    HazelcastInstance instance;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {{InMemoryFormat.BINARY}, {InMemoryFormat.OBJECT}});
    }

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
                    return new MyPortable();
                } else if (classId == 2) {
                    return new LittlePortable();
                }
                return null;
            }
        });
        return config;
    }

    private JsonObject createNameAgeOnDuty(String name, int age, boolean onDuty) {
        JsonObject object = Json.object();
        object.add("name", name);
        object.add("age", age);
        object.add("onDuty", onDuty);
        return object;
    }

    private HazelcastJsonValue putJsonString(Map map, String name, int age, boolean onDuty) {
        String f = createNameAgeOnDuty(name, age, onDuty).toString();
        HazelcastJsonValue json = HazelcastJson.fromString(f);
        map.put(name, json);
        return json;
    }

    private String putWithJsonStringKey(Map map, String name, int age, boolean onDuty) {
        String f = createNameAgeOnDuty(name, age, onDuty).toString();
        HazelcastJsonValue json = HazelcastJson.fromString(f);
        map.put(json, name);
        return name;
    }

    private HazelcastJsonValue putJsonString(Map map, String key, JsonValue value) {
        HazelcastJsonValue hazelcastJson = HazelcastJson.fromString(value.toString());
        map.put(key, hazelcastJson);
        return hazelcastJson;
    }

    private String putWithJsonStringKey(Map map, JsonValue key, String value) {
        HazelcastJsonValue lazyKey = HazelcastJson.fromString(key.toString());
        map.put(lazyKey, value);
        return value;
    }

    @Test
    public void testQueryOnNumberProperty() {
        IMap<String, JsonValue> map = instance.getMap(randomMapName());

        HazelcastJsonValue p1 = putJsonString(map, "a", 30, true);
        HazelcastJsonValue p2 = putJsonString(map, "b", 20, false);
        HazelcastJsonValue p3 = putJsonString(map, "c", 10, true);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("age", 20));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testQueryOnNumberPropertyOnKey() {
        IMap<JsonValue, String> map = instance.getMap(randomMapName());

        String p1 = putWithJsonStringKey(map, "a", 30, true);
        String p2 = putWithJsonStringKey(map, "b", 20, false);
        String p3 = putWithJsonStringKey(map, "c", 10, true);

        Collection<String> vals = map.values(Predicates.greaterEqual("__key.age", 20));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testQueryOnNumberProperty_whenSomeEntriesDoNotHaveTheField_shouldNotFail() {
        IMap<String, JsonValue> map = instance.getMap(randomMapName());

        JsonValue val1 = createNameAgeOnDuty("a", 30, true);
        val1.asObject().add("email", "a@aa.com");
        JsonValue val2 = createNameAgeOnDuty("b", 20, false);
        JsonValue val3 = createNameAgeOnDuty("c", 10, true);

        HazelcastJsonValue p1 = putJsonString(map, "a", val1);
        HazelcastJsonValue p2 = putJsonString(map, "b", val2);
        HazelcastJsonValue p3 = putJsonString(map, "c", val3);

        Collection<JsonValue> vals = map.values(Predicates.equal("email", "a@aa.com"));

        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testQueryOnNumberPropertyOnKey_whenSomeEntriesDoNotHaveTheField_shouldNotFail() {
        IMap<JsonValue, String> map = instance.getMap(randomMapName());

        JsonValue val1 = createNameAgeOnDuty("a", 30, true);
        val1.asObject().add("email", "a@aa.com");
        JsonValue val2 = createNameAgeOnDuty("b", 20, false);
        JsonValue val3 = createNameAgeOnDuty("c", 10, true);

        String p1 = putWithJsonStringKey(map, val1, "a");
        String p2 = putWithJsonStringKey(map, val2, "b");
        String p3 = putWithJsonStringKey(map, val3, "c");

        Collection<String> vals = map.values(Predicates.equal("__key.email", "a@aa.com"));

        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testQueryOnStringProperty() {
        IMap<String, JsonValue> map = instance.getMap(randomMapName());

        HazelcastJsonValue p1 = putJsonString(map, "a", 30, true);
        HazelcastJsonValue p2 = putJsonString(map, "b", 20, false);
        HazelcastJsonValue p3 = putJsonString(map, "c", 10, true);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("name", "b"));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p2));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testQueryOnBooleanProperty() {
        IMap<String, JsonValue> map = instance.getMap(randomMapName());

        HazelcastJsonValue p1 = putJsonString(map, "a", 30, true);
        HazelcastJsonValue p2 = putJsonString(map, "b", 20, false);
        HazelcastJsonValue p3 = putJsonString(map, "c", 10, true);

        Collection<JsonValue> vals = map.values(Predicates.equal("onDuty", true));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testQueryOnArrayIndex() {
        JsonObject value1 = Json.object();
        JsonObject value2 = Json.object();
        JsonObject value3 = Json.object();
        JsonArray array1 = Json.array(new int[]{1, 2, 3, 4, 5});
        JsonArray array2 = Json.array(new int[]{10, 20, 30, 40, 50});
        JsonArray array3 = Json.array(new int[]{100, 200, 300, 400, 500});
        value1.add("numbers", array1);
        value2.add("numbers", array2);
        value3.add("numbers", array3);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", value1);
        HazelcastJsonValue p2 = putJsonString(map, "two", value2);
        HazelcastJsonValue p3 = putJsonString(map, "three", value3);

        Collection<String> vals = map.keySet(Predicates.greaterEqual("numbers[1]", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains("two"));
        assertTrue(vals.contains("three"));
    }

    @Test
    public void testQueryOnArrayIndexOnKey() {
        JsonObject value1 = Json.object();
        JsonObject value2 = Json.object();
        JsonObject value3 = Json.object();
        JsonArray array1 = Json.array(new int[]{1, 2, 3, 4, 5});
        JsonArray array2 = Json.array(new int[]{10, 20, 30, 40, 50});
        JsonArray array3 = Json.array(new int[]{100, 200, 300, 400, 500});
        value1.add("numbers", array1);
        value2.add("numbers", array2);
        value3.add("numbers", array3);

        IMap<JsonValue, String> map = instance.getMap(randomMapName());
        String p1 = putWithJsonStringKey(map, value1, "one");
        String p2 = putWithJsonStringKey(map, value2, "two");
        String p3 = putWithJsonStringKey(map, value3, "three");

        Collection<String> vals = map.values(Predicates.greaterEqual("__key.numbers[1]", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p2));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testNestedQuery() {
        JsonObject object1 = Json.object();
        JsonObject nested1 = Json.object();
        JsonObject object2 = Json.object();
        JsonObject nested2 = Json.object();

        nested1.add("lim", 5);
        nested2.add("lim", 6);

        object1.add("inner", nested1);
        object2.add("inner", nested2);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", object1);
        HazelcastJsonValue p2 = putJsonString(map, "two", object2);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("inner.lim", 6));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testNestedQuery_whenOneObjectMissingFirstLevelProperty() {
        JsonObject object1 = Json.object();
        JsonObject nested1 = Json.object();
        JsonObject object2 = Json.object();
        JsonObject nested2 = Json.object();

        nested1.add("lim", 5);
        nested2.add("someotherlim", 6);

        object1.add("inner", nested1);
        object2.add("inner", nested2);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", object1);
        HazelcastJsonValue p2 = putJsonString(map, "two", object2);

        Collection<JsonValue> vals = map.values(Predicates.lessEqual("inner.lim", 6));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testArrayInNestedQuery() {
        JsonObject object1 = Json.object();
        JsonObject nested1 = Json.object();
        JsonObject object2 = Json.object();
        JsonObject nested2 = Json.object();
        JsonArray array1 = Json.array(new int[]{1, 2, 3, 4, 5, 6});
        JsonArray array2 = Json.array(new int[]{10, 20, 30, 40, 50, 60});

        nested1.add("arr", array1);
        nested2.add("arr", array2);

        object1.add("inner", nested1);
        object2.add("inner", nested2);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", object1);
        HazelcastJsonValue p2 = putJsonString(map, "two", object2);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("inner.arr[2]", 20));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testArrayInNestedQuery_whenOneArrayIsShort_shouldNotThrow() {
        JsonObject object1 = Json.object();
        JsonObject nested1 = Json.object();
        JsonObject object2 = Json.object();
        JsonObject nested2 = Json.object();
        JsonArray array1 = Json.array(new int[]{1, 2, 3, 4, 5, 6});
        JsonArray array2 = Json.array(new int[]{10});

        nested1.add("arr", array1);
        nested2.add("arr", array2);

        object1.add("inner", nested1);
        object2.add("inner", nested2);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", object1);
        putJsonString(map, "two", object2);

        Collection<JsonValue> vals = map.values(Predicates.lessEqual("inner.arr[2]", 20));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testNestedQueryInArray() {
        JsonValue array1 = Json.array();
        array1.asArray().add(createNameAgeOnDuty("a", 50, false))
                .add(createNameAgeOnDuty("b", 30, true))
                .add(createNameAgeOnDuty("c", 32, true))
                .add(createNameAgeOnDuty("d", 17, false));
        JsonValue array2 = Json.array();
        array2.asArray().add(createNameAgeOnDuty("e", 10, false))
                .add(createNameAgeOnDuty("f", 20, true))
                .add(createNameAgeOnDuty("g", 30, true))
                .add(createNameAgeOnDuty("h", 40, false));
        JsonValue array3 = Json.array();
        array3.asArray().add(createNameAgeOnDuty("i", 26, false))
                .add(createNameAgeOnDuty("j", 24, true))
                .add(createNameAgeOnDuty("k", 1, true))
                .add(createNameAgeOnDuty("l", 90, false));
        JsonObject obj1 = Json.object();
        obj1.add("arr", array1);
        JsonObject obj2 = Json.object();
        obj2.add("arr", array2);
        JsonObject obj3 = Json.object();
        obj3.add("arr", array3);

        HazelcastJsonValue p1 = HazelcastJson.fromString(obj1.toString());
        HazelcastJsonValue p2 = HazelcastJson.fromString(obj2.toString());
        HazelcastJsonValue p3 = HazelcastJson.fromString(obj3.toString());

        IMap<String, HazelcastJsonValue> map = instance.getMap(randomMapName());
        map.put("one", p1);
        map.put("two", p2);
        map.put("three", p3);

        Collection<HazelcastJsonValue> vals = map.values(Predicates.greaterEqual("arr[2].age", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testQueryOnArray_whenAnyIsUsed() {
        JsonObject value1 = Json.object();
        JsonObject value2 = Json.object();
        JsonObject value3 = Json.object();
        JsonArray array1 = Json.array(new int[]{1, 2, 3, 4, 20});
        JsonArray array2 = Json.array(new int[]{10, 20, 30});
        JsonArray array3 = Json.array(new int[]{100, 200, 300, 400});
        value1.add("numbers", array1);
        value2.add("numbers", array2);
        value3.add("numbers", array3);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        putJsonString(map, "one", value1);
        HazelcastJsonValue p2 = putJsonString(map, "two", value2);
        HazelcastJsonValue p3 = putJsonString(map, "three", value3);

        Collection<JsonValue> vals = map.values(Predicates.greaterThan("numbers[any]", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p2));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testJsonValueIsJustANumber() {
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < 10; i++) {
            map.put(i, HazelcastJson.fromString(Json.value(i).toString()));
        }
        Collection<HazelcastJsonValue> vals = map.values(Predicates.greaterEqual("this", 3));
        assertEquals(7, vals.size());
        for (HazelcastJsonValue value : vals) {
            int intValue = Json.parse(value.toString()).asInt();
            assertTrue(intValue >= 3);
            assertGreaterOrEquals("predicate result ", intValue, 3);
        }
    }

    @Test
    public void testJsonValueIsJustAString() {
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < 10; i++) {
            map.put(i, HazelcastJson.fromString(Json.value("s" + i).toString()));
        }
        Collection<HazelcastJsonValue> vals = map.values(Predicates.greaterEqual("this", "s3"));
        assertEquals(7, vals.size());
        for (HazelcastJsonValue value : vals) {
            String stringVal = Json.parse(value.toString()).asString();
            assertTrue(stringVal.compareTo("s3") >= 0);
        }
    }

    @Test
    public void testJsonValueIsJustABoolean() {
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < 10; i++) {
            map.put(i, HazelcastJson.fromString(Json.value(i < 7).toString()));
        }
        Collection<Map.Entry<Integer, HazelcastJsonValue>> vals = map.entrySet(Predicates.equal("this", true));
        assertEquals(7, vals.size());
        for (Map.Entry<Integer, HazelcastJsonValue> entry : vals) {
            assertTrue(entry.getKey() < 7);
            assertEquals("true", entry.getValue().toString());
        }

    }

    @Test
    public void testNestedQueryInArray_whenAnyMatchesMultipleNestedObjects_shouldReturnAllMatching() {
        JsonValue array1 = Json.array();
        array1.asArray().add(createNameAgeOnDuty("a", 50, false))
                .add(createNameAgeOnDuty("b", 30, true))
                .add(createNameAgeOnDuty("c", 32, true))
                .add(createNameAgeOnDuty("d", 17, false));
        JsonValue array2 = Json.array();
        array2.asArray().add(createNameAgeOnDuty("e", 10, false))
                .add(createNameAgeOnDuty("f", 20, true))
                .add(createNameAgeOnDuty("g", 30, true))
                .add(createNameAgeOnDuty("h", 40, false));
        JsonValue array3 = Json.array();
        array3.asArray().add(createNameAgeOnDuty("i", 26, false))
                .add(createNameAgeOnDuty("j", 24, true))
                .add(createNameAgeOnDuty("k", 1, true))
                .add(createNameAgeOnDuty("l", 90, false));
        JsonObject obj1 = Json.object();
        obj1.add("arr", array1);
        JsonObject obj2 = Json.object();
        obj2.add("arr", array2);
        JsonObject obj3 = Json.object();
        obj3.add("arr", array3);

        IMap<String, HazelcastJsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", obj1);
        HazelcastJsonValue p2 = putJsonString(map, "two", obj2);
        HazelcastJsonValue p3 = putJsonString(map, "three", obj3);

        Collection<HazelcastJsonValue> vals = map.values(Predicates.greaterThan("arr[any].age", 40));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testArrayInsideArray() {
        JsonValue array1 = Json.array();
        array1.asArray().add(Json.array(new int[]{1, 2, 3, 4})).add(Json.array(new int[]{10, 20, 30, 40}));
        JsonObject obj1 = Json.object();
        obj1.add("arr", array1);

        System.out.println(obj1);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        HazelcastJsonValue p1 = putJsonString(map, "one", obj1);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("arr[1][3]", 20));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testJsonPredicateOnKey() {
        JsonValue array1 = Json.array();
        array1.asArray().add(createNameAgeOnDuty("a", 50, false))
                .add(createNameAgeOnDuty("b", 30, true))
                .add(createNameAgeOnDuty("c", 32, true))
                .add(createNameAgeOnDuty("d", 17, false));
        JsonValue array2 = Json.array();
        array2.asArray().add(createNameAgeOnDuty("e", 10, false))
                .add(createNameAgeOnDuty("f", 20, true))
                .add(createNameAgeOnDuty("g", 30, true))
                .add(createNameAgeOnDuty("h", 40, false));
        JsonValue array3 = Json.array();
        array3.asArray().add(createNameAgeOnDuty("i", 26, false))
                .add(createNameAgeOnDuty("j", 24, true))
                .add(createNameAgeOnDuty("k", 1, true))
                .add(createNameAgeOnDuty("l", 90, false));
        JsonObject obj1 = Json.object();
        obj1.add("arr", array1);
        JsonObject obj2 = Json.object();
        obj2.add("arr", array2);
        JsonObject obj3 = Json.object();
        obj3.add("arr", array3);

        HazelcastJsonValue p1 = HazelcastJson.fromString(obj1.toString());
        HazelcastJsonValue p2 = HazelcastJson.fromString(obj2.toString());
        HazelcastJsonValue p3 = HazelcastJson.fromString(obj3.toString());

        IMap<HazelcastJsonValue, String> map = instance.getMap(randomMapName());
        map.put(p1, "one");
        map.put(p2, "two");
        map.put(p3, "three");

        Collection<String> vals = map.values(Predicates.greaterEqual("__key.arr[2].age", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains("one"));
        assertTrue(vals.contains("two"));
    }

    public static class MyPortable implements Portable {

        private LittlePortable[] littlePortables;

        public MyPortable() {

        }

        public MyPortable(LittlePortable[] littlePortables) {
            this.littlePortables = littlePortables;
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
            writer.writePortableArray("littlePortables", this.littlePortables);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.littlePortables = (LittlePortable[]) reader.readPortableArray("littlePortables");
        }
    }

    public static class LittlePortable implements Portable {

        private int real;
        private int[] tempReals;

        public LittlePortable() {

        }

        public LittlePortable(int real, int[] tempReals) {
            this.real = real;
            this.tempReals = tempReals;
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
            writer.writeInt("real", this.real);
            writer.writeIntArray("tempReals", this.tempReals);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.real = reader.readInt("real");
            this.tempReals = reader.readIntArray("tempReals");
        }
    }
}
