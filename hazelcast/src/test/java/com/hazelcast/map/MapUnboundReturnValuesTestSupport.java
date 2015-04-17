package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.MapQueryResultSizeLimitHelper;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.util.EmptyStatement;

import java.util.Set;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

abstract class MapUnboundReturnValuesTestSupport extends HazelcastTestSupport {

    protected static final int CLUSTER_SIZE = 20;
    protected static final int PARTITION_COUNT = 271;

    protected static final int SMALL_LIMIT = MapQueryResultSizeLimitHelper.MINIMUM_MAX_RESULT_LIMIT;
    protected static final int MEDIUM_LIMIT = (int) (MapQueryResultSizeLimitHelper.MINIMUM_MAX_RESULT_LIMIT * 1.5);
    protected static final int LARGE_LIMIT = MapQueryResultSizeLimitHelper.MINIMUM_MAX_RESULT_LIMIT * 2;

    protected static final int PRE_CHECK_TRIGGER_LIMIT_INACTIVE = -1;
    protected static final int PRE_CHECK_TRIGGER_LIMIT_ACTIVE = Integer.MAX_VALUE;

    protected enum KeyType {
        STRING,
        INTEGER
    }

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance;
    private IMap<Object, Integer> map;

    private int configLimit;
    private int lowerLimit;
    private int upperLimit;
    private int checkLimitInterval;

    protected void runMapFullTest(int partitionCount, int clusterSize, int limit, int preCheckTrigger, KeyType keyType) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(keyType, lowerLimit);
        internalRunWithLowerBoundCheck(keyType);

        shutdown(factory, map);
    }

    protected void runMapQuickTest(int partitionCount, int clusterSize, int limit, int preCheckTrigger, KeyType keyType) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(keyType, upperLimit);
        internalRunQuick();
        System.out.println(
                format("Limit of %d exceeded at %d (%.2f)", configLimit, upperLimit, (upperLimit * 100f / configLimit)));

        shutdown(factory, map);
    }

    protected void runMapTxnWithExceptionTest(int partitionCount, int clusterSize, int limit, int preCheckTrigger) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(KeyType.INTEGER, upperLimit);
        internalRunTxnWithException();

        shutdown(factory, map);
    }

    protected void runMapTxnWithoutExceptionTest(int partitionCount, int clusterSize, int limit, int preCheckTrigger) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(KeyType.INTEGER, upperLimit);
        internalRunTxnWithoutException();

        shutdown(factory, map);
    }

    private void internalSetUp(int partitionCount, int clusterSize, int limit, int preCheckTrigger) {
        Config config = createConfig(partitionCount, limit, preCheckTrigger);
        factory = createTestHazelcastInstanceFactory(clusterSize);
        map = getMapWithNodeCount(clusterSize, config, factory);

        configLimit = limit;
        lowerLimit = Math.round(limit * 0.95f);
        upperLimit = Math.round(limit * 1.5f);
        checkLimitInterval = limit / 1000;
    }

    private Config createConfig(int partitionCount, int limit, int preCheckTrigger) {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
        config.setProperty(GroupProperties.PROP_QUERY_RESULT_SIZE_LIMIT, String.valueOf(limit));
        config.setProperty(GroupProperties.PROP_QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK, String.valueOf(preCheckTrigger));
        return config;
    }

    private TestHazelcastInstanceFactory createTestHazelcastInstanceFactory(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }
        return createHazelcastInstanceFactory(nodeCount);
    }

    private <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, Config config, TestHazelcastInstanceFactory factory) {
        String mapName = randomMapName();

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        while (nodeCount > 1) {
            factory.newHazelcastInstance(config);
            nodeCount--;
        }

        instance = factory.newHazelcastInstance(config);
        return instance.getMap(mapName);
    }

    private void mapPut(KeyType keyType, int index) {
        switch (keyType) {
            case STRING:
                map.put("key" + index, index);
                break;
            case INTEGER:
                map.put(index, index);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported keyType " + keyType);
        }
    }

    private void fillToLimit(KeyType keyType, int limit) {
        for (int index = 1; index <= limit; index++) {
            mapPut(keyType, index);
        }
        assertEquals("Expected map size of map to match limit " + limit, limit, map.size());
    }

    private void internalRunWithLowerBoundCheck(KeyType keyType) {
        // it should be safe to call IMap.keySet() within lowerLimit
        try {
            map.keySet();
        } catch (QueryResultSizeExceededException e) {
            fail(format("lowerLimit is too high, already got QueryResultSizeExceededException below %d", lowerLimit));
        }

        // check up to upperLimit on which index the QueryResultSizeExceededException is thrown by IMap.keySet()
        int index = lowerLimit;
        try {
            int keySetSize = 0;
            while (++index < upperLimit) {
                mapPut(keyType, index);
                if (index % checkLimitInterval == 0) {
                    Set<Object> keySet = map.keySet();
                    keySetSize = keySet.size();
                }
            }
            fail(format("Limit should have exceeded, but ran into upperLimit of %d with IMap.keySet() size of %d",
                    upperLimit, keySetSize));
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }
        System.out.println(format("Limit of %d exceeded at %d (%.2f)", configLimit, index, (index * 100f / configLimit)));

        assertTrue(format("QueryResultSizeExceededException should not trigger below limit of %d, but was %d (%.2f%%)",
                configLimit, index, (index * 100f / configLimit)), index > configLimit);

        // run over all methods to check if they trigger correctly
        internalRunQuick();
    }

    private void internalRunQuick() {
        try {
            map.values(TruePredicate.INSTANCE);
            failExpectedException("IMap.values(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            map.keySet(TruePredicate.INSTANCE);
            failExpectedException("IMap.keySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            map.entrySet(TruePredicate.INSTANCE);
            failExpectedException("IMap.entrySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            map.values();
            failExpectedException("IMap.values()");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            map.keySet();
            failExpectedException("IMap.keySet()");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            map.entrySet();
            failExpectedException("IMap.entrySet()");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }
    }

    private void internalRunTxnWithException() {
        TransactionContext transactionContext = instance.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMap<Object, Integer> txnMap = transactionContext.getMap(map.getName());

        try {
            txnMap.values(TruePredicate.INSTANCE);
            failExpectedException("TransactionalMap.values(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        try {
            txnMap.keySet(TruePredicate.INSTANCE);
            failExpectedException("TransactionalMap.keySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            EmptyStatement.ignore(e);
        }

        // there is no entrySet in TransactionalMap

        transactionContext.rollbackTransaction();
    }

    private void internalRunTxnWithoutException() {
        TransactionContext transactionContext = instance.newTransactionContext();
        transactionContext.beginTransaction();

        TransactionalMap<Object, Integer> txnMap = transactionContext.getMap(map.getName());

        try {
            assertEquals("TransactionalMap.values()", upperLimit, txnMap.values().size());
        } catch (QueryResultSizeExceededException e) {
            failUnwantedException("TransactionalMap.values()");
        }

        try {
            assertEquals("TransactionalMap.keySet()", upperLimit, txnMap.keySet().size());
        } catch (QueryResultSizeExceededException e) {
            failUnwantedException("TransactionalMap.keySet()");
        }

        // there is no entrySet in TransactionalMap

        transactionContext.rollbackTransaction();
    }

    private void failExpectedException(String methodName) {
        fail(format("Expected QueryResultSizeExceededException while calling %s with limit %d and upperLimit %d",
                methodName, configLimit, upperLimit));
    }

    private void failUnwantedException(String methodName) {
        fail(format("Unwanted QueryResultSizeExceededException was thrown while calling %s with limit %d and upperLimit %d",
                methodName, configLimit, upperLimit));
    }

    private void shutdown(TestHazelcastInstanceFactory factory, IMap map) {
        map.destroy();
        factory.terminateAll();
    }
}
