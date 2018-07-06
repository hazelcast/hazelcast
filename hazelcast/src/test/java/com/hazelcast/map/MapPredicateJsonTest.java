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
import com.hazelcast.core.JsonString;
import com.hazelcast.core.JsonStringImpl;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
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

    private JsonString putJsonString(Map map, String name, int age, boolean onDuty) {
        String f = createNameAgeOnDuty(name, age, onDuty).toString();
        JsonString json = new JsonStringImpl(f);
        map.put(name, json);
        return json;
    }

    private JsonString putJsonString(Map map, String key, JsonValue value) {
        JsonString json = new JsonStringImpl(value.toString());
        map.put(key, json);
        return json;
    }

    @Test
    public void testQueryOnNumberProperty() {
        IMap<String, JsonString> map = instance.getMap(randomMapName());

        JsonString p1 = putJsonString(map, "a", 30, true);
        JsonString p2 = putJsonString(map, "b", 20, false);
        JsonString p3 = putJsonString(map, "c", 10, true);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("age", 20));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p2));
    }

    @Test
    public void testQueryOnNumberProperty_whenSomeEntriesDoNotHaveTheField_shouldNotFail() {
        IMap<String, JsonString> map = instance.getMap(randomMapName());

        JsonValue val1 = createNameAgeOnDuty("a", 30, true);
        ((JsonObject) val1).add("email", "a@aa.com");
        JsonValue val2 = createNameAgeOnDuty("b", 20, false);
        JsonValue val3 = createNameAgeOnDuty("c", 10, true);

        JsonString p1 = putJsonString(map, "a", val1);
        JsonString p2 = putJsonString(map, "b", val2);
        JsonString p3 = putJsonString(map, "c", val3);

        Collection<JsonString> vals = map.values(Predicates.equal("email", "a@aa.com"));

        assertEquals(1, vals.size());
        assertTrue(vals.contains(p1));
    }

    @Test
    public void testQueryOnStringProperty() {
        IMap<String, JsonString> map = instance.getMap(randomMapName());

        JsonString p1 = putJsonString(map, "a", 30, true);
        JsonString p2 = putJsonString(map, "b", 20, false);
        JsonString p3 = putJsonString(map, "c", 10, true);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("name", "b"));

        assertEquals(2, vals.size());
        assertTrue(vals.contains(p2));
        assertTrue(vals.contains(p3));
    }

    @Test
    public void testQueryOnBooleanProperty() {
        IMap<String, JsonString> map = instance.getMap(randomMapName());

        JsonString p1 = putJsonString(map, "a", 30, true);
        JsonString p2 = putJsonString(map, "b", 20, false);
        JsonString p3 = putJsonString(map, "c", 10, true);

        Collection<JsonString> vals = map.values(Predicates.equal("onDuty", true));

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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", value1);
        JsonString p2 = putJsonString(map, "two", value2);
        JsonString p3 = putJsonString(map, "three", value3);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("numbers[1]", 20));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", object1);
        JsonString p2 = putJsonString(map, "two", object2);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("inner.lim", 6));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", object1);
        JsonString p2 = putJsonString(map, "two", object2);

        Collection<JsonString> vals = map.values(Predicates.lessEqual("inner.lim", 6));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", object1);
        JsonString p2 = putJsonString(map, "two", object2);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("inner.arr[2]", 20));
        assertEquals(1, vals.size());
        assertTrue(vals.contains(p2));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", object1);
        JsonString p2 = putJsonString(map, "two", object2);

        Collection<JsonString> vals = map.values(Predicates.lessEqual("inner.arr[2]", 20));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", obj1);
        JsonString p2 = putJsonString(map, "two", obj2);
        JsonString p3 = putJsonString(map, "three", obj3);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("arr[2].age", 20));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", value1);
        JsonString p2 = putJsonString(map, "two", value2);
        JsonString p3 = putJsonString(map, "three", value3);

        Collection<JsonString> vals = map.values(Predicates.greaterEqual("numbers[any]", 20));
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

        IMap<String, JsonString> map = instance.getMap(randomMapName());
        JsonString p1 = putJsonString(map, "one", obj1);
        JsonString p2 = putJsonString(map, "two", obj2);
        JsonString p3 = putJsonString(map, "three", obj3);

        Collection<JsonString> vals = map.values(Predicates.greaterThan("arr[any].age", 40));
        assertEquals(2, vals.size());
        assertTrue(vals.contains(p1));
        assertTrue(vals.contains(p3));
    }
}
