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

package com.hazelcast.query.impl;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class JsonIndexIntegrationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "map";

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public CacheDeserializedValues cacheDeserializedValues;

    @Parameterized.Parameter(2)
    public MetadataPolicy metadataPolicy;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, cacheDeserializedValues: {1}, metadataPolicy: {2}")
    public static Collection<Object[]> parametersInMemoryFormat() {
        List<Object[]> parameters = new ArrayList<Object[]>();
        for (InMemoryFormat imf: new InMemoryFormat[]{InMemoryFormat.OBJECT, InMemoryFormat.BINARY}) {
            for (CacheDeserializedValues cdv: CacheDeserializedValues.values()) {
                for (MetadataPolicy pp: MetadataPolicy.values()) {
                    parameters.add(new Object[]{imf, cdv, pp});
                }
            }
        }
        return parameters;
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig(MAP_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setMetadataPolicy(metadataPolicy)
                .setCacheDeserializedValues(cacheDeserializedValues);
        return config;
    }

    @Test
    public void testViaAccessingInternalIndexes() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(MAP_NAME);
        map.addIndex(IndexType.HASH, "age");
        map.addIndex(IndexType.HASH, "active");
        map.addIndex(IndexType.HASH, "name");

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, new HazelcastJsonValue(jsonString));
        }

        Set<QueryableEntry> records = getRecords(instance, MAP_NAME, "age", 40);
        assertEquals(1, records.size());

        records = getRecords(instance, MAP_NAME, "active", true);
        assertEquals(500, records.size());

        records = getRecords(instance, MAP_NAME, "name", "sancar");
        assertEquals(1000, records.size());
    }

    @Test
    public void testIndex_viaQueries() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(MAP_NAME);
        map.addIndex(IndexType.HASH, "age");
        map.addIndex(IndexType.HASH, "active");
        map.addIndex(IndexType.HASH, "name");

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, new HazelcastJsonValue(jsonString));
        }

        assertEquals(500, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", "true"))).size());
        assertEquals(299, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, map.values(Predicates.sql("name == sancar")).size());

    }

    @Test
    public void testEntryProcessorChanges_viaQueries() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(MAP_NAME);
        map.addIndex(IndexType.HASH, "age");
        map.addIndex(IndexType.HASH, "active");
        map.addIndex(IndexType.HASH, "name");

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, new HazelcastJsonValue(jsonString));
        }

        assertEquals(500, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", "true"))).size());
        assertEquals(299, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, map.values(Predicates.sql("name == sancar")).size());
        map.executeOnEntries(new JsonEntryProcessor());
        assertEquals(1000, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", false))).size());
        assertEquals(0, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", false))).size());
        assertEquals(1000, map.values(Predicates.sql("name == sancar")).size());

    }

    @Test
    public void testEntryProcessorChanges_viaQueries_withoutIndex() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(MAP_NAME);

        for (int i = 0; i < 1000; i++) {
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            map.put(i, new HazelcastJsonValue(jsonString));
        }

        assertEquals(500, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", "true"))).size());
        assertEquals(299, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, map.values(Predicates.sql("name == sancar")).size());
        map.executeOnEntries(new JsonEntryProcessor());
        assertEquals(1000, map.values(Predicates.and(Predicates.equal("name", "sancar"), Predicates.equal("active", false))).size());
        assertEquals(0, map.values(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", false))).size());
        assertEquals(1000, map.values(Predicates.sql("name == sancar")).size());

    }

    private static class JsonEntryProcessor implements EntryProcessor<Integer, HazelcastJsonValue, String> {

        @Override
        public String process(Map.Entry<Integer, HazelcastJsonValue> entry) {
            JsonObject jsonObject = Json.parse(entry.getValue().toString()).asObject();
            jsonObject.set("age", 0);
            jsonObject.set("active", false);

            entry.setValue(new HazelcastJsonValue(jsonObject.toString()));
            return "anyResult";
        }
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
        for (int partitionId : mapServiceContext.getOrInitCachedMemberPartitions()) {
            Indexes indexes = mapContainer.getIndexes(partitionId);

            for (InternalIndex index : indexes.getIndexes()) {
                for (String component : index.getComponents()) {
                    if (component.equals(IndexUtils.canonicalizeAttribute(attribute))) {
                        result.add(index);

                        break;
                    }
                }
            }
        }
        return result;
    }
}




