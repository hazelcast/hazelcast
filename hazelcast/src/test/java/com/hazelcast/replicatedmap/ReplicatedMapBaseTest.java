package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.test.HazelcastTestSupport;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public abstract class ReplicatedMapBaseTest extends HazelcastTestSupport {

    protected static Field REPLICATED_MAP_SERVICE;

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


    @SuppressWarnings("unchecked")
    protected AbstractMap.SimpleEntry<Integer, Integer>[] buildTestValues() {
        Random random = new Random();
        AbstractMap.SimpleEntry<Integer, Integer>[] testValues = new AbstractMap.SimpleEntry[100];
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = new AbstractMap.SimpleEntry<Integer, Integer>(random.nextInt(), random.nextInt());
        }
        return testValues;
    }

    public List<ReplicatedMap> createMapOnEachInstance(HazelcastInstance[] instances, String replicatedMapName) {
        ArrayList<ReplicatedMap> maps = new ArrayList<ReplicatedMap>();
        for (int i = 0; i < instances.length; i++) {
            ReplicatedMap<Object, Object> replicatedMap = instances[i].getReplicatedMap(replicatedMapName);
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
}
