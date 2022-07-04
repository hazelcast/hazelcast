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

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
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
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapAggregationJsonTest extends HazelcastTestSupport {

    public static final int OBJECT_COUNT = 1000;
    private static final String STRING_PREFIX = "s";

    TestHazelcastInstanceFactory factory;
    HazelcastInstance instance;

    @Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public MetadataPolicy metadataPolicy;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, metadataPolicy: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, MetadataPolicy.OFF},
                {InMemoryFormat.BINARY, MetadataPolicy.CREATE_ON_UPDATE},
                {InMemoryFormat.OBJECT, MetadataPolicy.OFF},
                {InMemoryFormat.OBJECT, MetadataPolicy.CREATE_ON_UPDATE},
        });
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
        config.getMapConfig("default")
                .setInMemoryFormat(inMemoryFormat)
                .setMetadataPolicy(metadataPolicy);
        return config;
    }

    @Test
    public void testLongField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        long maxLongValue = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testDoubleField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        double maxDoubleValue = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>doubleMax("doubleValue"));
        assertEquals(OBJECT_COUNT - 0.5, maxDoubleValue, 0.00001);
    }

    @Test
    public void testStringField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        String maxStringValue = map.aggregate(Aggregators.comparableMax("stringValue"));
        assertEquals(STRING_PREFIX + "999", maxStringValue);
    }

    @Test
    public void testNestedField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        long maxLongValue = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>longMax("nestedObject.nestedLongValue"));
        assertEquals((OBJECT_COUNT - 1) * 10, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenObjectIsEmpty() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HazelcastJsonValue(Json.object().toString()));
        long maxLongValue = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathDoesNotExist() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HazelcastJsonValue(Json.object().add("someField", "someValue").toString()));
        long maxLongValue = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenValueIsNotAnObject() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HazelcastJsonValue(Json.value(5).toString()));
        long maxLongValue = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, maxLongValue);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HazelcastJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        long count = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>longMax("longValue"));
        assertEquals(OBJECT_COUNT - 1, count);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal_count() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HazelcastJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        long count = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>>count("longValue"));
        assertEquals(OBJECT_COUNT, count);
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal_distinct() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(OBJECT_COUNT, new HazelcastJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        Collection<Object> distinctLongValues = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>, Object>distinct("longValue"));
        assertEquals(OBJECT_COUNT, distinctLongValues.size());
    }

    @Test
    public void testAny() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        Collection<Object> distinctStrings = map.aggregate(Aggregators.<Map.Entry<Integer, HazelcastJsonValue>, Object>distinct("stringValueArray[any]"));
        assertEquals(OBJECT_COUNT * 2, distinctStrings.size());
        for (int i = 0; i < OBJECT_COUNT; i++) {
            assertContains(distinctStrings, "nested0 " + STRING_PREFIX + i);
            assertContains(distinctStrings, "nested1 " + STRING_PREFIX + i);
        }
    }

    protected IMap<Integer, HazelcastJsonValue> getPreloadedMap() {
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < OBJECT_COUNT; i++) {
            map.put(i, createHazelcastJsonValue(STRING_PREFIX + i, (long) i, (double) i + 0.5, (long) i * 10));
        }
        return map;
    }

    private HazelcastJsonValue createHazelcastJsonValue(String stringValue, long longValue, double doubleValue, long nestedLongValue) {
        return new HazelcastJsonValue(createJsonString(stringValue, longValue, doubleValue, nestedLongValue));
    }

    @Test
    public void testArrayWithNestedField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        long maxLongValue = map.aggregate(Aggregators.longMax(
                "nestedArray[any].nestedArrayObject.secondary.nestedObjectLongValue"));
        assertEquals((OBJECT_COUNT - 1) * 10, maxLongValue);
    }

    @Test
    public void testArrayWithNestedField_when_field_nonexist() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        Long maxLongValue = map.aggregate(Aggregators.longMax(
                "nestedArray[any].nestedArrayObject.secondary.nestedObjectLongValue.nonExistant"));
        assertNull(maxLongValue);
    }

    @Test
    public void testArrayWithNestedField_when_last_field_array() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        Long maxLongValue = map.aggregate(Aggregators.longMax(
                "nestedArray[any].nestedArrayObject.secondary"));
        assertNull(maxLongValue);
    }

    @Test
    public void testArrayWithNestedField_when_field_name_is_wrongly_written() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        Long maxLongValue = map.aggregate(Aggregators.longMax("nestedArray[any].nestedArrayObject.secondary.xnestedObjectLongValue"));
        assertNull(maxLongValue);
    }

    @Test
    public void test_nested_json() {
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(randomMapName());
        map.put(1, new HazelcastJsonValue(nestedJsonString()));
        Long sum = map.aggregate(Aggregators.longSum("list[any].secondLevelItem.thirdLevelItem"));

        assertEquals(6L, sum.longValue());
    }

    private static String nestedJsonString() {
        return "{\n"
                + "  \"list\": [\n"
                + "    {\n"
                + "      \"secondLevelItem\": {\n"
                + "        \"thirdLevelItem\": 1\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"secondLevelItem\": {\n"
                + "        \"thirdLevelItem\": 2\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"secondLevelItem\": {\n"
                + "        \"thirdLevelItem\": 3\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";
    }

    private String createJsonString(String stringValue, long longValue, double doubleValue, long nestedLongValue) {
        JsonObject object = Json.object();

        JsonArray array = new JsonArray();
        array.add(Json.object().add("nestedArrayObject",
                Json.object().add("secondary",
                        Json.object().add("nestedObjectLongValue", nestedLongValue))));


        object.add("nestedArray", array);

        object.add("stringValue", stringValue)
                .add("longValue", longValue)
                .add("doubleValue", doubleValue)
                .add("nestedObject", Json.object()
                        .add("nestedLongValue", nestedLongValue))
                .add("stringValueArray", Json.array("nested0 " + stringValue, "nested1 " + stringValue));
        return object.toString();
    }
}
