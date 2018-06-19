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

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IndexIntegrationTest extends HazelcastTestSupport {

    @Test
    public void loadFromStore_whenEvicted() {
        // GIVEN
        String name = randomString();
        String attributeName = "currency";
        String currency = "dollar";
        long amount = 5L;

        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        MapConfig mapConfig = config.getMapConfig(name);
        mapConfig.setEvictionPolicy(EvictionPolicy.LFU);
        mapConfig.setMinEvictionCheckMillis(0);
        // size=1 means each put/load will trigger eviction
        MaxSizeConfig maxSizeConfig = new MaxSizeConfig(1, PER_PARTITION);
        mapConfig.setMaxSizeConfig(maxSizeConfig);
        // Dummy map loader which returns a Trade object with amount=5, currency=dollar
        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new DummyLoader(amount, currency));

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, Trade> map = instance.getMap(name);
        map.addIndex(attributeName, false);

        // WHEN
        // This `get` will trigger load from map-loader but since eviction kicks in, entry will get removed
        // We should be able to get the value loaded from store but index should be removed
        Trade trade = map.get(randomString());
        map.get(randomString());

        // THEN
        assertEquals(1, map.size());
        assertEquals(5L, (long) trade.amount);
        assertEquals(currency, trade.currency);

        List<Index> indexes = getIndexOfAttributeForMap(instance, name, attributeName);
        Set<QueryableEntry> dollars = new HashSet<QueryableEntry>();
        for (Index index : indexes) {
            Set<QueryableEntry> result = index.getRecords(currency);
            if (result != null) {
                dollars.addAll(result);
            }
        }
        assertEquals(1, dollars.size());
    }

    @Test
    public void putRemove_withIndex_whereAttributeIsNull() {
        // GIVEN
        MapIndexConfig mapIndexConfig = new MapIndexConfig();
        mapIndexConfig.setAttribute("amount");
        mapIndexConfig.setOrdered(false);

        MapConfig mapConfig = new MapConfig().setName("map");
        mapConfig.addMapIndexConfig(mapIndexConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        Trade trade = new Trade();
        trade.setCurrency("EUR");
        trade.setAmount(null);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Trade> map = instance.getMap("map");

        // WHEN
        map.put(1, trade);
        map.remove(1);

        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("amount").isNull();
        Collection<Trade> values = map.values(predicate);

        // THEN
        assertEquals(0, values.size());
        assertNull(map.get(1));
    }

    @Test(timeout = 1000 * 60)
    public void putAndQuery_whenMultipleMappingFound_thenDoNotReturnDuplicatedEntry() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, Body> map = instance.getMap(randomMapName());
        map.addIndex("limbArray[any].fingerCount", true);

        Limb leftHand = new Limb("hand", new Nail("red"));
        Limb rightHand = new Limb("hand");
        Body body = new Body("body", leftHand, rightHand);

        map.put(1, body);

        Predicate predicate = Predicates.greaterEqual("limbArray[any].fingerCount", 0);
        Collection<Body> values = map.values(predicate);

        assertThat(values, hasSize(1));
    }

    @Test
    public void foo_methodGetters() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IMap<Integer, SillySequence> map = hazelcastInstance.getMap(randomName());

        SillySequence sillySequence = new SillySequence(0, 100);
        map.put(0, sillySequence);

        Predicate predicate = Predicates.equal("payload[any]", 3);
        Collection<SillySequence> result = map.values(predicate);
        assertThat(result, hasSize(1));
    }

    @Test
    public void foo_fieldGetters() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();
        IMap<Integer, SillySequence> map = hazelcastInstance.getMap(randomName());

        SillySequence sillySequence = new SillySequence(0, 100);
        map.put(0, sillySequence);

        Predicate predicate = Predicates.equal("payloadField[any]", 3);
        Collection<SillySequence> result = map.values(predicate);
        assertThat(result, hasSize(1));
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

    static class Body implements Serializable {
        String name;
        Limb[] limbArray = new Limb[0];
        Collection<Limb> limbCollection = new ArrayList<Limb>();

        Body(String name, Limb... limbs) {
            this.name = name;
            this.limbCollection = Arrays.asList(limbs);
            this.limbArray = limbs;
        }
    }

    static class Limb implements Serializable {
        String name;
        Nail[] nailArray = new Nail[0];
        int fingerCount;
        Collection<Nail> nailCollection = new ArrayList<Nail>();

        Limb(String name, Nail... nails) {
            this.name = name;
            this.nailCollection = Arrays.asList(nails);
            this.fingerCount = nails.length;
            this.nailArray = nails;
        }
    }

    static final class Nail implements Serializable {
        String colour;

        private Nail(String colour) {
            this.colour = colour;
        }
    }

    @SuppressWarnings("unused")
    public static class Trade implements Serializable {

        private String currency;
        private Long amount;

        public Trade() {
        }

        public String getCurrency() {
            return currency;
        }

        public void setCurrency(String currency) {
            this.currency = currency;
        }

        public Long getAmount() {
            return amount;
        }

        public void setAmount(Long amount) {
            this.amount = amount;
        }
    }

    @SuppressWarnings("unused")
    static class SillySequence implements DataSerializable {

        int count;
        Collection<Integer> payloadField;

        SillySequence() {
        }

        SillySequence(int from, int count) {
            this.count = count;
            this.payloadField = new ArrayList<Integer>(count);

            int to = from + count;
            for (int i = from; i < to; i++) {
                payloadField.add(i);
            }
        }

        public Collection<Integer> getPayload() {
            return payloadField;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(count);
            out.writeObject(payloadField);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            count = in.readInt();
            payloadField = in.readObject();
        }
    }

    static class DummyLoader implements MapLoader<String, Trade> {

        long amount;

        String currency;

        public DummyLoader(long amount, String currency) {
            this.amount = amount;
            this.currency = currency;
        }

        @Override
        public Trade load(String key) {
            Trade trade = new Trade();
            trade.setAmount(amount);
            trade.setCurrency(currency);
            return trade;
        }

        @Override
        public Map<String, Trade> loadAll(Collection<String> keys) {
            Map<String, Trade> map = new HashMap<String, Trade>();
            for (String key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            return null;
        }
    }
}
