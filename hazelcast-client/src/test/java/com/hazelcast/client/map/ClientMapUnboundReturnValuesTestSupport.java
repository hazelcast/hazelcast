package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapQueryResultSizeLimitHelper;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.util.EmptyStatement;

import java.util.UUID;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

abstract class ClientMapUnboundReturnValuesTestSupport {

    protected static final int PARTITION_COUNT = 271;

    protected static final int SMALL_LIMIT = MapQueryResultSizeLimitHelper.MINIMUM_MAX_RESULT_LIMIT;

    protected static final int PRE_CHECK_TRIGGER_LIMIT_INACTIVE = -1;
    protected static final int PRE_CHECK_TRIGGER_LIMIT_ACTIVE = Integer.MAX_VALUE;

    private IMap<Integer, Integer> serverMap;
    private IMap<Integer, Integer> clientMap;

    private int configLimit;
    private int upperLimit;

    protected void runClientMapTestWithException(int partitionCount, int limit, int preCheckTrigger) {
        internalSetUpClient(partitionCount, 1, limit, preCheckTrigger);

        fillToUpperLimit(serverMap, clientMap);
        internalRunWithException(clientMap);

        shutdown(serverMap);
    }

    protected void runClientMapTestWithoutException(int partitionCount, int limit, int preCheckTrigger) {
        internalSetUpClient(partitionCount, 1, limit, preCheckTrigger);

        fillToUpperLimit(serverMap, clientMap);
        internalRunWithoutException(clientMap);

        shutdown(serverMap);
    }

    private void internalSetUpClient(int partitionCount, int clusterSize, int limit, int preCheckTrigger) {
        Config config = createConfig(partitionCount, limit, preCheckTrigger);
        serverMap = getMapWithNodeCount(clusterSize, config);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        clientMap = client.getMap(serverMap.getName());

        configLimit = limit;
        upperLimit = Math.round(limit * 1.5f);
    }

    private Config createConfig(int partitionCount, int limit, int preCheckTrigger) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        config.setProperty(GroupProperties.PROP_QUERY_RESULT_SIZE_LIMIT, String.valueOf(limit));
        config.setProperty(GroupProperties.PROP_QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK, String.valueOf(preCheckTrigger));
        return config;
    }

    private <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, Config config) {
        String mapName = UUID.randomUUID().toString();

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        while (nodeCount > 1) {
            Hazelcast.newHazelcastInstance(config);
            nodeCount--;
        }

        HazelcastInstance node = Hazelcast.newHazelcastInstance(config);
        return node.getMap(mapName);
    }

    private void fillToUpperLimit(IMap<Integer, Integer> fillMap, IMap<Integer, Integer> queryMap) {
        for (int index = 1; index <= upperLimit; index++) {
            fillMap.put(index, index);
        }
        assertEquals("Expected map size of server map to match upperLimit", upperLimit, fillMap.size());
        assertEquals("Expected map size of client map to match upperLimit", upperLimit, queryMap.size());
    }

    private void internalRunWithException(IMap<Integer, Integer> queryMap) {
        try {
            queryMap.values(TruePredicate.INSTANCE);
            failExpectedException("IMap.values(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            queryMap.keySet(TruePredicate.INSTANCE);
            failExpectedException("IMap.keySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            queryMap.entrySet(TruePredicate.INSTANCE);
            failExpectedException("IMap.entrySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }
    }

    private void internalRunWithoutException(IMap<Integer, Integer> queryMap) {
        try {
            assertEquals("IMap.values()", upperLimit, queryMap.values().size());
        } catch (QueryResultSizeExceededException e) {
            failUnwantedException("IMap.values()");
        }

        try {
            assertEquals("IMap.keySet()", upperLimit, queryMap.keySet().size());
        } catch (QueryResultSizeExceededException e) {
            failUnwantedException("IMap.keySet()");
        }

        /*
        // FIXME: the performance of IMap.entrySet() is too bad to test it with the required number of entries
        try {
            assertEquals("IMap.entrySet()", upperLimit, queryMap.entrySet().size());
        } catch (QueryResultSizeExceededException e) {
            failUnwantedException("IMap.entrySet()");
        }
        */
    }

    private void failExpectedException(String methodName) {
        fail(format("Expected QueryResultSizeExceededException while calling %s with limit %d and upperLimit %d",
                methodName, configLimit, upperLimit));
    }

    private void failUnwantedException(String methodName) {
        fail(format("Unwanted QueryResultSizeExceededException was thrown while calling %s with limit %d and upperLimit %d",
                methodName, configLimit, upperLimit));
    }

    private void shutdown(IMap map) {
        map.destroy();
        Hazelcast.shutdownAll();
    }
}
