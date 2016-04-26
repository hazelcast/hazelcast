/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.client.quorum;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.quorum.QuorumTestUtil.getClientConfig;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientTransactionalMapQuorumTest extends HazelcastTestSupport {

    static PartitionedCluster cluster;
    private static final String MAP_NAME_PREFIX = "quorum";
    private static final String QUORUM_ID = "threeNodeQuorumRule";
    static HazelcastInstance c1;
    static HazelcastInstance c2;
    static HazelcastInstance c3;
    static HazelcastInstance c4;
    static HazelcastInstance c5;
    private static TestHazelcastFactory factory;


    @Parameterized.Parameter(0)
    public TransactionOptions options;


    @Parameterized.Parameters(name = "Executing: {0}")
    public static Collection<Object[]> parameters() {

        TransactionOptions localOption = TransactionOptions.getDefault();
        localOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        return Arrays.asList(
                new Object[]{twoPhaseOption}, //
                new Object[]{localOption} //
        );
    }

    @BeforeClass
    public static void initialize() throws InterruptedException {
        QuorumConfig quorumConfig = new QuorumConfig();
        quorumConfig.setEnabled(true);
        quorumConfig.setSize(3);
        quorumConfig.setName(QUORUM_ID);

        MapConfig mapConfig = new MapConfig(MAP_NAME_PREFIX + "*");
        mapConfig.setQuorumName(QUORUM_ID);
        factory = new TestHazelcastFactory();
        cluster = new PartitionedCluster(factory).partitionFiveMembersThreeAndTwo(mapConfig, quorumConfig);
        initializeClients();
        verifyClients();
    }

    private static void verifyClients() {
        assertClusterSizeEventually(3, c1);
        assertClusterSizeEventually(3, c2);
        assertClusterSizeEventually(3, c3);
        assertClusterSizeEventually(2, c4);
        assertClusterSizeEventually(2, c5);
    }

    private static void initializeClients() {
        c1 = factory.newHazelcastClient(getClientConfig(cluster.h1));
        c2 = factory.newHazelcastClient(getClientConfig(cluster.h2));
        c3 = factory.newHazelcastClient(getClientConfig(cluster.h3));
        c4 = factory.newHazelcastClient(getClientConfig(cluster.h4));
        c5 = factory.newHazelcastClient(getClientConfig(cluster.h5));
    }


    @AfterClass
    public static void killAllHazelcastInstances() throws IOException {
        factory.terminateAll();
    }


    @Test(expected = TransactionException.class)
    public void testTxPutThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.put("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxGetThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.get("foo");
        transactionContext.commitTransaction();
    }


    @Test(expected = TransactionException.class)
    public void testTxGetForUpdateThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.getForUpdate("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxRemoveThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.remove("foo");
        transactionContext.commitTransaction();
    }


    @Test(expected = TransactionException.class)
    public void testTxRemoveValueThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.remove("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxDeleteThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.delete("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxSetThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.set("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxPutWithTTLThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.put("foo", "bar", 10, TimeUnit.SECONDS);
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxPutIfAbsentThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.putIfAbsent("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxReplaceThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.replace("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxReplaceExpectedValueThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.replace("foo", "bar", "baz");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxSizeThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.size();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxContainsKeyThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.containsKey("foo");
        transactionContext.commitTransaction();
    }


    @Test(expected = TransactionException.class)
    public void testTxIsEmptyThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.isEmpty();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxKeySetThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.keySet();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxKeySetWithPredicateThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.keySet(TruePredicate.INSTANCE);
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxValuesThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.values();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void testTxValuesWithPredicateThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(randomMapName(MAP_NAME_PREFIX));
        map.values(TruePredicate.INSTANCE);
        transactionContext.commitTransaction();
    }

}
