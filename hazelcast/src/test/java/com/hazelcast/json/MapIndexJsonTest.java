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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.lessThan;
import static com.hazelcast.query.Predicates.notEqual;
import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapIndexJsonTest extends HazelcastTestSupport {

    public static final int OBJECT_COUNT = 1000;
    private static final String STRING_PREFIX = "s🙂榛";
    private static final String NON_INDEXED_MAP_NAME = "non-indexed-map";

    TestHazelcastInstanceFactory factory;
    HazelcastInstance instance;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public MetadataPolicy metadataPolicy;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, metadataPolicy: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {
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
        MapConfig mapConfig = new MapConfig("default")
                .setInMemoryFormat(inMemoryFormat)
                .setMetadataPolicy(metadataPolicy)
                .addIndexConfig(sortedIndexConfig("longValue"))
                .addIndexConfig(sortedIndexConfig("doubleValue"))
                .addIndexConfig(sortedIndexConfig("nestedObject.nestedLongValue"))
                .addIndexConfig(sortedIndexConfig("stringValue"))
                .addIndexConfig(sortedIndexConfig("stringValueArray"));

        MapConfig nonIndexedMapConfig = new MapConfig(NON_INDEXED_MAP_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setMetadataPolicy(metadataPolicy);

        config.addMapConfig(mapConfig)
                .addMapConfig(nonIndexedMapConfig);

        return config;
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

    @Test
    public void testDynamicIndexCreation() {
        IMap<Integer, HazelcastJsonValue> map = getPreloadedMap(NON_INDEXED_MAP_NAME);
        assertIndex(map, 100, 100.5, "doubleValue", false);
        map.addIndex(sortedIndexConfig("stringValue"));
        // Query on non-indexed field
        assertIndex(map, 200, 200, "longValue", false);
        // Query on indexed field
        assertIndex(map, 999, STRING_PREFIX + "999", "stringValue");
    }

    private void assertIndex(IMap<Integer, HazelcastJsonValue> map, int targetCount, Comparable comparable, String attributeName) {
        assertIndex(map, targetCount, comparable, attributeName, true);
    }

    private void assertIndex(IMap<Integer, HazelcastJsonValue> map, int targetCount, Comparable comparable,
                             String attributeName, boolean assertFromInternalIndexes) {
        int mapSize = map.size();
        assertEquals(targetCount, map.keySet(lessThan(attributeName, comparable)).size());
        assertEquals(mapSize - 1, map.keySet(notEqual(attributeName, comparable)).size());
        assertEquals(1, map.keySet(equal(attributeName, comparable)).size());

        Object keyFromPredicate = map.keySet(equal(attributeName, comparable)).iterator().next();
        Object valueFromPredicate = map.values(equal(attributeName, comparable)).iterator().next();

        assertEquals(valueFromPredicate, map.get(keyFromPredicate));

        if (assertFromInternalIndexes) {
            QueryableEntry queryableEntry = getRecordsFromInternalIndex(factory.getAllHazelcastInstances(),
                    map.getName(), attributeName, comparable).iterator().next();
            assertEquals(keyFromPredicate, queryableEntry.getKey());
            assertEquals(valueFromPredicate, queryableEntry.getValue());
        }
    }

    private IMap<Integer, HazelcastJsonValue> getPreloadedMap(String mapName) {
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(mapName);
        for (int i = 0; i < OBJECT_COUNT; i++) {
            map.put(i, createHazelcastJsonValue(STRING_PREFIX + i, i, (double) i + 0.5, i));
        }
        return map;
    }

    private IMap<Integer, HazelcastJsonValue> getPreloadedMap() {
        return getPreloadedMap(randomMapName());
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
}
