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

package com.hazelcast.map;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPredicateJsonTest extends HazelcastTestSupport {

    TestHazelcastInstanceFactory factory;
    HazelcastInstance instance;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(3);
        factory.newInstances();
        instance = factory.getAllHazelcastInstances().iterator().next();
    }

    private JsonObject createNameAgeOnDuty(String name, int age, boolean onDuty) {
        JsonObject object = Json.object();
        object.add("name", name);
        object.add("age", age);
        object.add("onDuty", onDuty);
        return object;
    }

    private JsonValue putJsonString(Map map, String name, int age, boolean onDuty) {
        String f = createNameAgeOnDuty(name, age, onDuty).toString();
        JsonValue json = Json.parse(f);
        map.put(name, json);
        return json;
    }

    private String putWithJsonStringKey(Map map, String name, int age, boolean onDuty) {
        String f = createNameAgeOnDuty(name, age, onDuty).toString();
        JsonValue json = Json.parse(f);
        map.put(json, name);
        return name;
    }

    private JsonValue putJsonString(Map map, String key, JsonValue value) {
        map.put(key, value);
        return value;
    }

    private String putWithJsonStringKey(Map map, JsonValue key, String value) {
        map.put(key, value);
        return value;
    }

    @Test
    public void testQueryOnNumberProperty() {
        IMap<String, JsonValue> map = instance.getMap(randomMapName());

        JsonValue p1 = putJsonString(map, "a", 30, true);
        JsonValue p2 = putJsonString(map, "b", 20, false);
        JsonValue p3 = putJsonString(map, "c", 10, true);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("age", 20));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));

        vals = map.values(Predicates.greaterEqual("age", 20));
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
        ((JsonObject) val1).add("email", "a@aa.com");
        JsonValue val2 = createNameAgeOnDuty("b", 20, false);
        JsonValue val3 = createNameAgeOnDuty("c", 10, true);

        JsonValue p1 = putJsonString(map, "a", val1);
        JsonValue p2 = putJsonString(map, "b", val2);
        JsonValue p3 = putJsonString(map, "c", val3);

        Collection<JsonValue> vals = map.values(Predicates.equal("email", "a@aa.com"));

        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testQueryOnNumberPropertyOnKey_whenSomeEntriesDoNotHaveTheField_shouldNotFail() {
        IMap<JsonValue, String> map = instance.getMap(randomMapName());

        JsonValue val1 = createNameAgeOnDuty("a", 30, true);
        ((JsonObject) val1).add("email", "a@aa.com");
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

        JsonValue p1 = putJsonString(map, "a", 30, true);
        JsonValue p2 = putJsonString(map, "b", 20, false);
        JsonValue p3 = putJsonString(map, "c", 10, true);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("name", "b"));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p2));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testQueryOnBooleanProperty() {
        IMap<String, JsonValue> map = instance.getMap(randomMapName());

        JsonValue p1 = putJsonString(map, "a", 30, true);
        JsonValue p2 = putJsonString(map, "b", 20, false);
        JsonValue p3 = putJsonString(map, "c", 10, true);

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
        JsonArray array1 = Json.array(1, 2, 3, 4, 5);
        JsonArray array2 = Json.array(10, 20, 30, 40, 50);
        JsonArray array3 = Json.array(100, 200, 300, 400, 500);
        value1.add("numbers", array1);
        value2.add("numbers", array2);
        value3.add("numbers", array3);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", value1);
        JsonValue p2 = putJsonString(map, "two", value2);
        JsonValue p3 = putJsonString(map, "three", value3);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("numbers[1]", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p2));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testQueryOnArrayIndexOnKey() {
        JsonObject value1 = Json.object();
        JsonObject value2 = Json.object();
        JsonObject value3 = Json.object();
        JsonArray array1 = Json.array(1, 2, 3, 4, 5);
        JsonArray array2 = Json.array(10, 20, 30, 40, 50);
        JsonArray array3 = Json.array(100, 200, 300, 400, 500);
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
        JsonValue p1 = putJsonString(map, "one", object1);
        JsonValue p2 = putJsonString(map, "two", object2);

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
        JsonValue p1 = putJsonString(map, "one", object1);
        JsonValue p2 = putJsonString(map, "two", object2);

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
        JsonArray array1 = Json.array(1, 2, 3, 4, 5, 6);
        JsonArray array2 = Json.array(10, 20, 30, 40, 50, 60);

        nested1.add("arr", array1);
        nested2.add("arr", array2);

        object1.add("inner", nested1);
        object2.add("inner", nested2);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", object1);
        JsonValue p2 = putJsonString(map, "two", object2);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("inner.arr[2]", 20));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p2));
    }

    //json libraries generally do not support duplicate keys. we may not support it either
    @Test
    public void testObjectHasOneMatchingOneNonMatchingValueOnSamePath() {
        JsonObject obj1 = Json.object();
        obj1.add("data", createNameAgeOnDuty("a", 50, false));
        obj1.add("data", createNameAgeOnDuty("a", 30, false));

        Iterator<JsonObject.Member> it = obj1.iterator();
        for (; it.hasNext(); ) {
            JsonObject.Member m = it.next();
            System.out.println(m.getValue().toString());
        }
        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", obj1);

        Collection<JsonValue> vals = map.values(Predicates.greaterThan("data.age", 40));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testArrayInNestedQuery_whenOneArrayIsShort_shouldNotThrow() {
        JsonObject object1 = Json.object();
        JsonObject nested1 = Json.object();
        JsonObject object2 = Json.object();
        JsonObject nested2 = Json.object();
        JsonArray array1 = Json.array(1, 2, 3, 4, 5, 6);
        JsonArray array2 = Json.array(10);

        nested1.add("arr", array1);
        nested2.add("arr", array2);

        object1.add("inner", nested1);
        object2.add("inner", nested2);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", object1);
        JsonValue p2 = putJsonString(map, "two", object2);

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

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", obj1);
        JsonValue p2 = putJsonString(map, "two", obj2);
        JsonValue p3 = putJsonString(map, "three", obj3);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("arr[2].age", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testQueryOnArray_whenAnyIsUsed() {
        JsonObject value1 = Json.object();
        JsonObject value2 = Json.object();
        JsonObject value3 = Json.object();
        JsonArray array1 = Json.array(1, 2, 3, 4, 20);
        JsonArray array2 = Json.array(10, 20, 30);
        JsonArray array3 = Json.array(100, 200, 300, 400);
        value1.add("numbers", array1);
        value2.add("numbers", array2);
        value3.add("numbers", array3);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", value1);
        JsonValue p2 = putJsonString(map, "two", value2);
        JsonValue p3 = putJsonString(map, "three", value3);

        Collection<JsonValue> vals = map.values(Predicates.greaterEqual("numbers[any]", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));
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

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", obj1);
        JsonValue p2 = putJsonString(map, "two", obj2);
        JsonValue p3 = putJsonString(map, "three", obj3);

        Collection<JsonValue> vals = map.values(Predicates.greaterThan("arr[any].age", 40));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testArrayInsideArray() {
        JsonValue array1 = Json.array();
        array1.asArray().add(Json.array(1, 2, 3, 4)).add(Json.array(10, 20, 30, 40));
        JsonObject obj1 = Json.object();
        obj1.add("arr", array1);

        System.out.println(obj1);

        IMap<String, JsonValue> map = instance.getMap(randomMapName());
        JsonValue p1 = putJsonString(map, "one", obj1);

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

        IMap<JsonValue, String> map = instance.getMap(randomMapName());
        map.put(obj1, "one");
        map.put(obj2, "two");
        map.put(obj3, "three");

        Collection<String> vals = map.values(Predicates.greaterEqual("__key.arr[2].age", 20));
        assertEquals(2, vals.size());
        assertTrue(vals.contains("one"));
        assertTrue(vals.contains("two"));
    }

    @Test
    public void testPortableAny() {
        LittlePortable lp1 = new LittlePortable(1);
        LittlePortable lp2 = new LittlePortable(2);
        LittlePortable lp3 = new LittlePortable(3);
        LittlePortable lp4 = new LittlePortable(4);
        LittlePortable lp5 = new LittlePortable(5);

        LittlePortable[] lps1 = new LittlePortable[]{lp1, lp2, lp3};
        LittlePortable[] lps2 = new LittlePortable[]{lp4, lp5};

        MyPortable mp1 = new MyPortable(lps1);
        MyPortable mp2 = new MyPortable(lps2);

        IMap<String, MyPortable> map = instance.getMap(randomMapName());
        map.put("one", mp1);
        map.put("two", mp2);

        Collection<MyPortable> vals = map.values(Predicates.lessEqual("littlePortables[any].real", 2));

        assertEquals(1, vals.size());
    }

    @Test
    public void testAny() {
        Integer[] arr1 = new Integer[]{1, 2, 3, 4};
        Integer[] arr2 = new Integer[]{4, 5, 6, 7};

        IntStore ints1 = new IntStore();
        IntStore ints2 = new IntStore();
        ints1.ints = arr1;
        ints2.ints = arr2;

        IMap<String, IntStore> map = instance.getMap(randomMapName());
        map.put("one", ints1);
        map.put("two", ints2);

        Collection<IntStore> vals = map.values(Predicates.lessEqual("ints[any]", 4));
        assertEquals(2, vals.size());
    }

    @Test
    public void testAny2() {
        Integer[] arr1 = new Integer[]{1, 2, 3, 4};
        Integer[] arr2 = new Integer[]{4, 5, 6, 7};

        IntStore ints1 = new IntStore();
        IntStore ints2 = new IntStore();
        ints1.ints = arr1;
        ints2.ints = arr2;

        IMap<String, IntStore> map = instance.getMap(randomMapName());
        map.put("one", ints1);
        map.put("two", ints2);

        Collection<IntStore> vals = map.values(Predicates.lessEqual("ints[2]", 4));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(ints1));
    }

    @Test
    public void testAny2OnKey() {
        Integer[] arr1 = new Integer[]{1, 2, 3, 4};
        Integer[] arr2 = new Integer[]{4, 5, 6, 7};

        IntStore ints1 = new IntStore();
        IntStore ints2 = new IntStore();
        ints1.ints = arr1;
        ints2.ints = arr2;

        IMap<IntStore, String> map = instance.getMap(randomMapName());
        map.put(ints1, "one");
        map.put(ints2, "two");

        Collection<String> vals = map.values(Predicates.lessEqual("__key.ints[2]", 4));
        assertEquals(1, vals.size());
        assertTrue(vals.contains("one"));
    }

    @Test
    @Ignore
    public void testArrayOnKey() {
        Integer[] arr1 = new Integer[]{1, 2, 3, 4};
        Integer[] arr2 = new Integer[]{4, 5, 6, 7};

        IMap<Integer[], String> map = instance.getMap(randomMapName());
        map.put(arr1, "one");
        map.put(arr2, "two");

        Collection<String> vals = map.values(Predicates.lessEqual("__key[2]", 4));
        assertEquals(1, vals.size());
        assertTrue(vals.contains("one"));
    }


    @Test
    @Ignore
    public void testArray() {
        Integer[] arr1 = new Integer[]{1, 2, 3, 4};
        Integer[] arr2 = new Integer[]{4, 5, 6, 7};

        IMap<String, Integer[]> map = instance.getMap(randomMapName());
        map.put("one", arr1);
        map.put("two", arr2);

        Collection<Integer[]> vals = map.values(Predicates.lessEqual("this[2]", 4));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(arr1));
    }

    private static class IntStore implements Serializable {
        private Integer[] ints;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IntStore intStore = (IntStore) o;
            return Arrays.equals(ints, intStore.ints);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(ints);
        }
    }

    private static class MyPortable implements Portable {

        private LittlePortable[] littlePortables;

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

    private static class LittlePortable implements Portable {

        private int real;

        public LittlePortable(int real) {
            this.real = real;
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
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.real = reader.readInt("real");
        }
    }
}
