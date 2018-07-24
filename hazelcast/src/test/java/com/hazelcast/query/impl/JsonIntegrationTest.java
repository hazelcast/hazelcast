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

package com.hazelcast.query.impl;

import com.hazelcast.json.Json;
import com.hazelcast.json.JsonValue;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class JsonIntegrationTest extends HazelcastTestSupport {

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT},
        });
    }

    @Test
    public void testViaAccessingInternalIndexes() {
        Config config = new Config();
        String mapName = "map";
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, JsonValue> map = instance.getMap(mapName);
        map.addIndex("age", false);
        map.addIndex("active", false);
        map.addIndex("name", false);

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, Json.parse(jsonString));
        }

        Set<QueryableEntry> records = getRecords(instance, mapName, "age", 40);
        assertEquals(1, records.size());

        records = getRecords(instance, mapName, "active", true);
        assertEquals(500, records.size());

        records = getRecords(instance, mapName, "name", "sancar");
        assertEquals(1000, records.size());
    }

    private Set<QueryableEntry> getRecords(HazelcastInstance instance, String mapName, String attribute, Comparable value) {
        List<Index> indexes = getIndexOfAttributeForMap(instance, mapName, attribute);
        Set<QueryableEntry> records = new HashSet<QueryableEntry>();
        for (Index index : indexes) {
            records.addAll(index.getRecords(value));
        }
        return records;
    }

    private static List<Index> getIndexOfAttributeForMap(HazelcastInstance instance, String mapName, String attribute) {
        Node node = getNode(instance);
        MapService service = node.nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);

        List<Index> result = new ArrayList<Index>();
        for (int partitionId : mapServiceContext.getOwnedPartitions()) {
            Indexes indexes = mapContainer.getIndexes(partitionId);
            result.add(indexes.getIndex(attribute));
        }
        return result;
    }

    @Test
    public void testIndex_ViaQueries() {
        Config config = new Config();
        String mapName = "map";
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat).setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, JsonValue> map = instance.getMap(mapName);
        map.addIndex("age", false);
        map.addIndex("active", false);
        map.addIndex("name", false);

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, Json.parse(jsonString));
        }

        assertEquals(500, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", "true"))).size());
        assertEquals(299, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, map.values(new SqlPredicate("name == sancar")).size());

    }

    public static class JsonEntryProcessor implements EntryProcessor<Integer, JsonValue> {

        @Override
        public Object process(Map.Entry<Integer, JsonValue> entry) {
            JsonValue jsonValue = entry.getValue();
            jsonValue.asObject().set("age", 0);
            jsonValue.asObject().set("active", false);

            entry.setValue(jsonValue);
            return "anyResult";
        }

        @Override
        public EntryBackupProcessor<Integer, JsonValue> getBackupProcessor() {
            return new EntryBackupProcessor<Integer, JsonValue>() {
                @Override
                public void processBackup(Map.Entry<Integer, JsonValue> entry) {
                    process(entry);
                }
            };
        }
    }

    @Test
    public void testEntryProcessorChanges_ViaQueries() {
        Config config = new Config();
        String mapName = "map";
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat).setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, JsonValue> map = instance.getMap(mapName);
        map.addIndex("age", false);
        map.addIndex("active", false);
        map.addIndex("name", false);

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, Json.parse(jsonString));
        }

        assertEquals(500, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", "true"))).size());
        assertEquals(299, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, map.values(new SqlPredicate("name == sancar")).size());
        map.executeOnEntries(new JsonEntryProcessor());
        assertEquals(1000, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", false))).size());
        assertEquals(0, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", false))).size());
        assertEquals(1000, map.values(new SqlPredicate("name == sancar")).size());

    }

    @Test
    public void testEntryProcessorChanges_ViaQueries_withoutIndex() {
        Config config = new Config();
        String mapName = "map";
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat).setCacheDeserializedValues(CacheDeserializedValues.ALWAYS);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, JsonValue> map = instance.getMap(mapName);

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, Json.parse(jsonString));
        }

        assertEquals(500, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", "true"))).size());
        assertEquals(299, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, map.values(new SqlPredicate("name == sancar")).size());
        map.executeOnEntries(new JsonEntryProcessor());
        assertEquals(1000, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", false))).size());
        assertEquals(0, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", false))).size());
        assertEquals(1000, map.values(new SqlPredicate("name == sancar")).size());

    }

}




