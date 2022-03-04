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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.query.QueryResultSizeLimiter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.Set;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

abstract class MapUnboundedReturnValuesTestSupport extends HazelcastTestSupport {

    static final int TEN_MINUTES_IN_MILLIS = 10 * 60 * 1000;
    protected static final int CLUSTER_SIZE = 5;
    protected static final int PARTITION_COUNT = 271;

    protected static final int SMALL_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT;
    protected static final int MEDIUM_LIMIT = (int) (QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT * 1.5);
    protected static final int LARGE_LIMIT = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT * 2;

    protected static final int PRE_CHECK_TRIGGER_LIMIT_INACTIVE = -1;
    protected static final int PRE_CHECK_TRIGGER_LIMIT_ACTIVE = Integer.MAX_VALUE;
    protected TestHazelcastInstanceFactory factory;
    protected HazelcastInstance instance;
    protected IMap<Object, Integer> map;
    protected ILogger logger;
    protected int configLimit;
    protected int lowerLimit;
    protected int upperLimit;
    protected int checkLimitInterval;

    /**
     * Extensive test which ensures that the {@link QueryResultSizeExceededException} will not be thrown under the configured
     * limit.
     * <p>
     * This test fills the map below the configured limit to ensure that the exception is not triggered yet. Then it fills up the
     * map and periodically checks via {@link IMap#keySet()} if the exception is thrown. If the exception is triggered all other
     * methods from {@link IMap} are executed to ensure they trigger the exception, too.
     * <p>
     * This method fails if the exception is already thrown at {@link #lowerLimit} or if it is not thrown at {@link #upperLimit}.
     *
     * @param partitionCount  number of partitions the created cluster
     * @param clusterSize     number of nodes in the cluster
     * @param limit           result size limit which will be configured for the cluster
     * @param preCheckTrigger number of partitions which will be used for local pre-check, <tt>-1</tt> deactivates the pre-check
     * @param keyType         key type used for the map
     */
    protected void runMapFullTest(int partitionCount, int clusterSize, int limit, int preCheckTrigger, KeyType keyType) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(keyType, lowerLimit);
        internalRunWithLowerBoundCheck(keyType);

        shutdown(factory, map);
    }

    /**
     * Quick test which just calls the {@link IMap} methods once and check for the {@link QueryResultSizeExceededException}.
     * <p>
     * This test fills the map to an amount where the exception is safely triggered. Then all {@link IMap} methods are called
     * and checked if they trigger the exception.
     * <p>
     * This methods fails if any of the called methods does not trigger the exception.
     *
     * @param partitionCount  number of partitions the created cluster
     * @param clusterSize     number of nodes in the cluster
     * @param limit           result size limit which will be configured for the cluster
     * @param preCheckTrigger number of partitions which will be used for local pre-check, <tt>-1</tt> deactivates the pre-check
     * @param keyType         key type used for the map
     */
    protected void runMapQuickTest(int partitionCount, int clusterSize, int limit, int preCheckTrigger, KeyType keyType) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(keyType, upperLimit);
        internalRunQuick();
        internalRunLocalKeySet();
        logger.info(format("Limit of %d exceeded at %d (%.2f)", configLimit, upperLimit, (upperLimit * 100f / configLimit)));

        shutdown(factory, map);
    }

    /**
     * Test which calls {@link TransactionalMap} methods which are expected to throw {@link QueryResultSizeExceededException}.
     * <p>
     * This test fills the map to an amount where the exception is safely triggered. Then all {@link TransactionalMap} methods are
     * called which should trigger the exception.
     * <p>
     * This methods fails if any of the called methods does not trigger the exception.
     *
     * @param partitionCount  number of partitions the created cluster
     * @param clusterSize     number of nodes in the cluster
     * @param limit           result size limit which will be configured for the cluster
     * @param preCheckTrigger number of partitions which will be used for local pre-check, <tt>-1</tt> deactivates the pre-check
     */
    protected void runMapTxn(int partitionCount, int clusterSize, int limit, int preCheckTrigger) {
        internalSetUp(partitionCount, clusterSize, limit, preCheckTrigger);

        fillToLimit(KeyType.INTEGER, upperLimit);
        internalRunTxn();

        shutdown(factory, map);
    }

    private void internalSetUp(int partitionCount, int clusterSize, int limit, int preCheckTrigger) {
        Config config = createConfig(partitionCount, limit, preCheckTrigger);
        factory = createTestHazelcastInstanceFactory(clusterSize);
        map = getMapWithNodeCount(config, factory);

        configLimit = limit;
        lowerLimit = Math.round(limit * 0.95f);
        upperLimit = Math.round(limit * 1.5f);
        checkLimitInterval = limit / 1000;
    }

    private Config createConfig(int partitionCount, int limit, int preCheckTrigger) {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), valueOf(partitionCount));
        config.setProperty(ClusterProperty.QUERY_RESULT_SIZE_LIMIT.getName(), valueOf(limit));
        config.setProperty(ClusterProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getName(), valueOf(preCheckTrigger));
        return config;
    }

    private TestHazelcastInstanceFactory createTestHazelcastInstanceFactory(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }
        return createHazelcastInstanceFactory(nodeCount);
    }

    protected <K, V> IMap<K, V> getMapWithNodeCount(Config config, TestHazelcastInstanceFactory factory) {
        String name = randomString();
        MapConfig mapConfig = config.getMapConfig(name);
        mapConfig.setName(name);
        mapConfig.setAsyncBackupCount(0);
        mapConfig.setBackupCount(0);

        HazelcastInstance[] instances = factory.newInstances(config);
        instance = instances[0];
        logger = instance.getLoggingService().getLogger(getClass());

        assertClusterSizeEventually(factory.getCount(), instance);
        assertAllInSafeState(asList(instances));

        return instance.getMap(name);
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

    private void checkException(QueryResultSizeExceededException e) {
        String exception = ExceptionUtil.toString(e);
        if (exception.contains("QueryPartitionOperation")) {
            fail("QueryResultSizeExceededException was thrown by QueryPartitionOperation:\n" + exception);
        }
    }

    private void failExpectedException(String methodName) {
        fail(format("Expected QueryResultSizeExceededException while calling %s with limit %d and upperLimit %d",
                methodName, configLimit, upperLimit));
    }

    /**
     * Extensive test which ensures that the {@link QueryResultSizeExceededException} will not be thrown under the configured
     * limit.
     * <p>
     * This method requires the map to be filled to an amount where the exception is not triggered yet. This method then fills the
     * map and calls {@link IMap#keySet()} periodically to determine the limit on which the exception is thrown for the first
     * time. After that it runs {@link #internalRunQuick()} to ensure that all methods will trigger at this limit.
     * <p>
     * This method fails if the exception is already thrown at {@link #lowerLimit} or if it is not thrown at {@link #upperLimit}.
     */
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
            checkException(e);
        }
        logger.info(format("Limit of %d exceeded at %d (%.2f)", configLimit, index, (index * 100f / configLimit)));

        assertTrue(format("QueryResultSizeExceededException should not trigger below limit of %d, but was %d (%.2f%%)",
                configLimit, index, (index * 100f / configLimit)), index > configLimit);

        // do the quick check to ensure that all methods trigger at the actual limit
        internalRunQuick();
    }

    /**
     * Quick run which just executes the {@link IMap} methods once and check for the {@link QueryResultSizeExceededException}.
     * <p>
     * This method requires the map to be filled to an amount where the exception is safely triggered. The local running methods
     * {@link IMap#localKeySet()} and {@link IMap#localKeySet(Predicate)} are excluded, since they may need a higher fill rate to
     * succeed.
     * <p>
     * This methods fails if any of the called methods does not trigger the exception.
     */
    private void internalRunQuick() {
        try {
            map.values(Predicates.alwaysTrue());
            failExpectedException("IMap.values(predicate)");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            map.keySet(Predicates.alwaysTrue());
            failExpectedException("IMap.keySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            map.entrySet(Predicates.alwaysTrue());
            failExpectedException("IMap.entrySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            map.values();
            failExpectedException("IMap.values()");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            map.keySet();
            failExpectedException("IMap.keySet()");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            map.entrySet();
            failExpectedException("IMap.entrySet()");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }
    }

    /**
     * Quick run on the {@link IMap#localKeySet()} and {@link IMap#localKeySet(Predicate)} methods.
     * <p>
     * Requires the map to be filled so the exception is triggered even locally.
     * <p>
     * This methods fails if any of the called methods does not trigger the exception.
     */
    private void internalRunLocalKeySet() {
        try {
            map.localKeySet();
            failExpectedException("IMap.localKeySet()");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            map.localKeySet(Predicates.alwaysTrue());
            failExpectedException("IMap.localKeySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }
    }

    /**
     * Calls {@link TransactionalMap} methods once which are expected to throw {@link QueryResultSizeExceededException}.
     * <p>
     * This method requires the map to be filled to an amount where the exception is safely triggered.
     * <p>
     * This methods fails if any of the called methods does not trigger the exception.
     */
    private void internalRunTxn() {
        TransactionContext transactionContext = instance.newTransactionContext();
        transactionContext.beginTransaction();
        TransactionalMap<Object, Integer> txnMap = transactionContext.getMap(map.getName());

        try {
            txnMap.values(Predicates.alwaysTrue());
            failExpectedException("TransactionalMap.values(predicate)");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            txnMap.keySet(Predicates.alwaysTrue());
            failExpectedException("TransactionalMap.keySet(predicate)");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            txnMap.values();
            failExpectedException("TransactionalMap.values()");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        try {
            txnMap.keySet();
            failExpectedException("TransactionalMap.keySet()");
        } catch (QueryResultSizeExceededException e) {
            checkException(e);
        }

        transactionContext.rollbackTransaction();
    }

    private void shutdown(TestHazelcastInstanceFactory factory, IMap map) {
        map.destroy();
        factory.terminateAll();
    }

    protected enum KeyType {
        STRING,
        INTEGER
    }
}
