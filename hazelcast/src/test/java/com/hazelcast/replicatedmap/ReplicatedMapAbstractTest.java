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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.test.HazelcastTestSupport;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("WeakerAccess")
public abstract class ReplicatedMapAbstractTest extends HazelcastTestSupport {

    protected static final Field REPLICATED_MAP_SERVICE;

    static {
        try {
            REPLICATED_MAP_SERVICE = ReplicatedMapProxy.class.getDeclaredField("service");
            REPLICATED_MAP_SERVICE.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Config buildConfig(InMemoryFormat inMemoryFormat) {
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("default");
        replicatedMapConfig.setInMemoryFormat(inMemoryFormat);
        return config;
    }

    protected Config buildConfig(Config config, InMemoryFormat inMemoryFormat) {
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("default");
        replicatedMapConfig.setInMemoryFormat(inMemoryFormat);
        return config;
    }

    protected void assertMatchSuccessfulOperationQuota(double quota, int completeOps, int... values) {
        float[] quotas = new float[values.length];
        Object[] args = new Object[values.length + 1];
        args[0] = quota;

        for (int i = 0; i < values.length; i++) {
            quotas[i] = (float) values[i] / completeOps;
            args[i + 1] = quotas[i];
        }

        boolean success = true;
        for (int i = 0; i < values.length; i++) {
            if (quotas[i] < quota) {
                success = false;
                break;
            }
        }

        if (!success) {
            StringBuilder sb = new StringBuilder("Quote (%s) for updates not reached,");
            for (int i = 0; i < values.length; i++) {
                sb.append(" map").append(i + 1).append(": %s,");
            }
            sb.deleteCharAt(sb.length() - 1);
            fail(String.format(sb.toString(), args));
        }
    }

    @SuppressWarnings("unchecked")
    protected <K, V> ReplicatedRecord<K, V> getReplicatedRecord(ReplicatedMap<K, V> map, K key) throws Exception {
        ReplicatedMapProxy<K, V> proxy = (ReplicatedMapProxy<K, V>) map;
        ReplicatedMapService service = (ReplicatedMapService) REPLICATED_MAP_SERVICE.get(proxy);
        ReplicatedRecordStore store = service.getReplicatedRecordStore(map.getName(), false, key);
        return store.getReplicatedRecord(key);
    }

    @SuppressWarnings("unchecked")
    protected <K, V> ReplicatedRecordStore getStore(ReplicatedMap<K, V> map, K key) throws Exception {
        ReplicatedMapProxy<K, V> proxy = (ReplicatedMapProxy<K, V>) map;
        ReplicatedMapService service = (ReplicatedMapService) REPLICATED_MAP_SERVICE.get(proxy);
        return service.getReplicatedRecordStore(map.getName(), false, key);
    }

    public List<ReplicatedMap<String, Object>> createMapOnEachInstance(HazelcastInstance[] instances, String replicatedMapName) {
        ArrayList<ReplicatedMap<String, Object>> maps = new ArrayList<ReplicatedMap<String, Object>>();
        for (HazelcastInstance instance : instances) {
            ReplicatedMap<String, Object> replicatedMap = instance.getReplicatedMap(replicatedMapName);
            maps.add(replicatedMap);
        }
        return maps;
    }

    public ArrayList<Integer> generateRandomIntegerList(int count) {
        final ArrayList<Integer> keys = new ArrayList<Integer>();
        final Random random = new Random();
        for (int i = 0; i < count; i++) {
            keys.add(random.nextInt());
        }
        return keys;
    }

    protected Set<String> generateRandomKeys(HazelcastInstance instance, int partitionCount) {
        Set<String> keys = new HashSet<String>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            keys.add(generateKeyForPartition(instance, partitionId));
        }

        Set<Integer> partitionIds = new HashSet<Integer>();
        for (String key : keys) {
            assertTrue(partitionIds.add(getPartitionId(instance, key)));
        }

        return keys;
    }

    @SuppressWarnings("unchecked")
    protected AbstractMap.SimpleEntry<String, String>[] buildTestValues(Set<String> keys) {
        AbstractMap.SimpleEntry<String, String>[] testValues = new AbstractMap.SimpleEntry[keys.size()];
        int i = 0;
        for (String key : keys) {
            testValues[i++] = new AbstractMap.SimpleEntry<String, String>(key, key);
        }
        return testValues;
    }
}
