package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicationPublisher;
import com.hazelcast.test.HazelcastTestSupport;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Random;

import static org.junit.Assert.fail;

public abstract class ReplicatedMapBaseTest extends HazelcastTestSupport {

    protected static Field REPLICATED_RECORD_STORE;

    static {
        try {
            REPLICATED_RECORD_STORE = ReplicatedMapProxy.class.getDeclaredField("replicatedRecordStore");
            REPLICATED_RECORD_STORE.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Config buildConfig(InMemoryFormat inMemoryFormat, long replicationDelay) {
        Config config = new Config();
        ReplicatedMapConfig replicatedMapConfig = config.getReplicatedMapConfig("default");
        replicatedMapConfig.setReplicationDelayMillis(replicationDelay);
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
        return ((AbstractReplicatedRecordStore<K, V>) REPLICATED_RECORD_STORE.get(proxy)).getReplicatedRecord(key);
    }

    @SuppressWarnings("unchecked")
    protected <K, V> ReplicationPublisher<K, V> getReplicationPublisher(ReplicatedMap<K, V> map) throws Exception {
        ReplicatedMapProxy<K, V> proxy = (ReplicatedMapProxy<K, V>) map;
        return ((AbstractReplicatedRecordStore<K, V>) REPLICATED_RECORD_STORE.get(proxy)).getReplicationPublisher();
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
}
