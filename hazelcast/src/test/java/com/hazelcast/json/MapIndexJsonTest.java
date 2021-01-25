/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexJsonTest extends HazelcastTestSupport {

    public static final int OBJECT_COUNT = 1000;
    private static final String STRING_PREFIX = "s";
    private static final Collection<Object[]> mapConfigOptions = asList(new Object[][]{
            {InMemoryFormat.BINARY, MetadataPolicy.OFF},
            {InMemoryFormat.BINARY, MetadataPolicy.CREATE_ON_UPDATE},
            {InMemoryFormat.OBJECT, MetadataPolicy.OFF},
            {InMemoryFormat.OBJECT, MetadataPolicy.CREATE_ON_UPDATE},
    });

    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance instance;

    @Parameterized.Parameter
    public String mapName;

    @Parameterized.Parameters(name = "mapName: {0}")
    public static Collection<Object[]> parameters() {
        return mapConfigOptions.stream()
                .map(option -> new Object[]{Arrays.toString(option)})
                .collect(Collectors.toList());
    }

    @BeforeClass
    public static void beforeClass() {
        Config config = createConfig(mapConfigOptions);
        startInstances(config);
    }

    @AfterClass
    public static void afterClass() {
        factory.terminateAll();
    }

    protected static Config createConfig(Collection<Object[]> mapConfigOptions) {
        Config config = smallInstanceConfig();
        mapConfigOptions.forEach(option -> {
            MapConfig mapConfig = new MapConfig(Arrays.toString(option) + "*")
                    .setInMemoryFormat((InMemoryFormat) option[0])
                    .setMetadataPolicy((MetadataPolicy) option[1]);
            addIndexConfig(mapConfig);
            config.addMapConfig(mapConfig);
        });
        return config;
    }

    protected static void startInstances(Config config) {
        factory = new TestHazelcastInstanceFactory();
        factory.newInstances(config, 3);
        instance = factory.getAllHazelcastInstances().iterator().next();
        warmUpPartitions(factory.getAllHazelcastInstances());
    }

    private static void addIndexConfig(MapConfig config) {
        config.addIndexConfig(sortedIndexConfig("longValue"))
                .addIndexConfig(sortedIndexConfig("doubleValue"))
                .addIndexConfig(sortedIndexConfig("nestedObject.nestedLongValue"))
                .addIndexConfig(sortedIndexConfig("stringValue"))
                .addIndexConfig(sortedIndexConfig("stringValueArray"));
    }

    private static IndexConfig sortedIndexConfig(String attribute) {
        return new IndexConfig(IndexType.SORTED, attribute).setName(attribute);
    }

    @Test
    public void testLongField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        assertIndex(map, 100, 100, "longValue");
    }

    @Test
    public void testDoubleField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        assertIndex(map, 100, 100.5, "doubleValue");
    }

    @Test
    public void testStringField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        assertIndex(map, 999, STRING_PREFIX + "999", "stringValue");
    }

    @Test
    public void testNestedField() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        assertIndex(map, 100, 100, "nestedObject.nestedLongValue");
    }

    @Test
    public void testValueIsOmitted_whenObjectIsEmpty() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(0, new HazelcastJsonValue(Json.object().toString()));
        assertIndex(map, 99, 100, "longValue");
    }

    @Test
    public void testValueIsOmitted_whenAttributePathDoesNotExist() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(0, new HazelcastJsonValue(Json.object().add("someField", "someValue").toString()));
        assertIndex(map, 99, 100, "longValue");
    }

    @Test
    public void testValueIsOmitted_whenValueIsNotAnObject() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(0, new HazelcastJsonValue(Json.value(5).toString()));
        assertIndex(map, 99, 100, "longValue");
    }

    @Test
    public void testValueIsOmitted_whenAttributePathIsNotTerminal() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        map.put(0, new HazelcastJsonValue(Json.object()
                .add("longValue", Json.object())
                .toString()));
        assertIndex(map, 99, 100, "longValue");
    }

    @Test
    public void testAny() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap();
        String attributeName = "stringValueArray[any]";
        Comparable<String> comparable = "nested0 " + STRING_PREFIX + "999";

        int mapSize = map.size();
        assertEquals(999, map.keySet(lessThan(attributeName, comparable)).size());
        assertEquals(mapSize - 1, map.keySet(notEqual(attributeName, comparable)).size());
        assertEquals(1, map.keySet(equal(attributeName, comparable)).size());

        Object keyFromPredicate = map.keySet(equal(attributeName, comparable)).iterator().next();
        Object valueFromPredicate = map.values(equal(attributeName, comparable)).iterator().next();

        assertEquals(valueFromPredicate, map.get(keyFromPredicate));
    }

    protected void assertIndex(IMap<Integer, HazelcastJsonValue> map, int targetCount, Comparable comparable, String attributeName) {
        int mapSize = map.size();
        assertEquals(targetCount, map.keySet(lessThan(attributeName, comparable)).size());
        assertEquals(mapSize - 1, map.keySet(notEqual(attributeName, comparable)).size());
        assertEquals(1, map.keySet(equal(attributeName, comparable)).size());

        Object keyFromPredicate = map.keySet(equal(attributeName, comparable)).iterator().next();
        Object valueFromPredicate = map.values(equal(attributeName, comparable)).iterator().next();

        assertEquals(valueFromPredicate, map.get(keyFromPredicate));

        QueryableEntry queryableEntry = getRecordsFromInternalIndex(factory.getAllHazelcastInstances(), map.getName(), attributeName, comparable).iterator().next();

        assertEquals(keyFromPredicate, queryableEntry.getKey());
        assertEquals(valueFromPredicate, queryableEntry.getValue());
    }

    protected IMap<Integer, HazelcastJsonValue> getPreloadedMap() {
        IMap<Integer, HazelcastJsonValue> map = getMap();
        for (int i = 0; i < OBJECT_COUNT; i++) {
            map.put(i, createHazelcastJsonValue(STRING_PREFIX + i, i, (double) i + 0.5, (long) i));
        }
        return map;
    }

    private HazelcastJsonValue createHazelcastJsonValue(String stringValue, long longValue, double doubleValue, long nestedLongValue) {
        return new HazelcastJsonValue(createJsonString(stringValue, longValue, doubleValue, nestedLongValue));
    }

    private String createJsonString(String stringValue, long longValue, double doubleValue, long nestedLongValue) {
        JsonObject object = Json.object();
        object.add("stringValue", stringValue)
                .add("longValue", longValue)
                .add("doubleValue", doubleValue)
                .add("nestedObject", Json.object()
                        .add("nestedLongValue", nestedLongValue))
                .add("stringValueArray", Json.array("nested0 " + stringValue, "nested1 " + stringValue));
        return object.toString();
    }

    protected Set<QueryableEntry> getRecordsFromInternalIndex(Collection<HazelcastInstance> instances, String mapName, String attribute, Comparable value) {
        Set<QueryableEntry> records = new HashSet<>();
        for (HazelcastInstance instance : instances) {
            List<Index> indexes = getIndexOfAttributeForMap(instance, mapName, attribute);
            for (Index index : indexes) {
                records.addAll(index.getRecords(value));
            }
        }
        return records;
    }

    protected static List<Index> getIndexOfAttributeForMap(HazelcastInstance instance, String mapName, String attribute) {
        Node node = getNode(instance);
        MapService service = node.nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);

        List<Index> result = new ArrayList<>();
        for (int partitionId : mapServiceContext.getOrInitCachedMemberPartitions()) {
            Indexes indexes = mapContainer.getIndexes(partitionId);
            result.add(indexes.getIndex(attribute));
        }
        return result;
    }

    private <K, V> IMap<K, V> getMap() {
        return instance.getMap(mapName + randomMapName());
    }
}
