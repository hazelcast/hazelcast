/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.quorum;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.quorum.PartitionedCluster;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

    private static final String MAP_NAME_PREFIX = "quorum";
    private static final String QUORUM_ID = "threeNodeQuorumRule";

    static PartitionedCluster cluster;
    static HazelcastInstance c1;
    static HazelcastInstance c2;
    static HazelcastInstance c3;
    static HazelcastInstance c4;
    static HazelcastInstance c5;

    private static TestHazelcastFactory factory;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameter(0)
    public TransactionOptions options;

    @Parameterized.Parameters(name = "Options: {0}")
    public static Collection<Object[]> parameters() {

        TransactionOptions localOption = TransactionOptions.getDefault();
        localOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        return Arrays.asList(
                new Object[]{twoPhaseOption},
                new Object[]{localOption}
        );
    }

    @BeforeClass
    public static void initialize() throws Exception {
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

    private static void initializeClients() {
        c1 = factory.newHazelcastClient(getClientConfig(cluster.h1));
        c2 = factory.newHazelcastClient(getClientConfig(cluster.h2));
        c3 = factory.newHazelcastClient(getClientConfig(cluster.h3));
        c4 = factory.newHazelcastClient(getClientConfig(cluster.h4));
        c5 = factory.newHazelcastClient(getClientConfig(cluster.h5));
    }

    private static void verifyClients() {
        assertClusterSizeEventually(3, c1);
        assertClusterSizeEventually(3, c2);
        assertClusterSizeEventually(3, c3);
        assertClusterSizeEventually(2, c4);
        assertClusterSizeEventually(2, c5);
    }

    @AfterClass
    public static void killAllHazelcastInstances() {
        factory.terminateAll();
    }

    @Test
    public void testTxPutThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.put("foo", "bar");
    }

    @Test
    public void testTxPutSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.put("foo", "bar");
        transaction.commitTransaction();
    }

    @Test
    public void testTxGetThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.get("foo");
    }

    @Test
    public void testTxGetSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.get("foo");
        transaction.commitTransaction();
    }

    @Test
    public void testTxGetForUpdateThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.getForUpdate("foo");
    }

    @Test
    public void testTxGetForUpdateSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.getForUpdate("foo");
        transaction.commitTransaction();
    }

    @Test
    public void testTxRemoveThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.remove("foo");
    }

    @Test
    public void testTxRemoveSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.remove("foo");
        transaction.commitTransaction();
    }

    @Test
    public void testTxRemoveValueThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.remove("foo", "bar");
    }

    @Test
    public void testTxRemoveValueSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.remove("foo", "bar");
        transaction.commitTransaction();
    }

    @Test
    public void testTxDeleteThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.delete("foo");
    }

    @Test
    public void testTxDeleteSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.delete("foo");
        transaction.commitTransaction();
    }

    @Test
    public void testTxSetThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.set("foo", "bar");
    }

    @Test
    public void testTxSetSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.set("foo", "bar");
        transaction.commitTransaction();
    }

    @Test
    public void testTxPutWithTTLThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.put("foo", "bar", 10, TimeUnit.SECONDS);
    }

    @Test
    public void testTxPutWithTTLSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.put("foo", "bar", 10, TimeUnit.SECONDS);
        transaction.commitTransaction();
    }

    @Test
    public void testTxPutIfAbsentThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.putIfAbsent("foo", "bar");
    }

    @Test
    public void testTxPutIfAbsentSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.putIfAbsent("foo", "bar");
        transaction.commitTransaction();
    }

    @Test
    public void testTxReplaceThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.replace("foo", "bar");
    }

    @Test
    public void testTxReplaceSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.replace("foo", "bar");
        transaction.commitTransaction();
    }

    @Test
    public void testTxReplaceExpectedValueThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.replace("foo", "bar", "baz");
    }

    @Test
    public void testTxReplaceExpectedValueSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.replace("foo", "bar", "baz");
        transaction.commitTransaction();
    }

    @Test
    public void testTxSizeThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.size();
    }

    @Test
    public void testTxSizeSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.size();
        transaction.commitTransaction();
    }

    @Test
    public void testTxContainsKeyThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.containsKey("foo");
    }

    @Test
    public void testTxContainsKeySucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.containsKey("foo");
        transaction.commitTransaction();
    }

    @Test
    public void testTxIsEmptyThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.isEmpty();
    }

    @Test
    public void testTxIsEmptySucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.isEmpty();
        transaction.commitTransaction();
    }

    @Test
    public void testTxKeySetThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.keySet();
    }

    @Test
    public void testTxKeySetSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.keySet();
        transaction.commitTransaction();
    }

    @Test
    public void testTxKeySetWithPredicateThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.keySet(TruePredicate.INSTANCE);
    }

    @Test
    public void testTxKeySetWithPredicateSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.keySet(TruePredicate.INSTANCE);
        transaction.commitTransaction();
    }

    @Test
    public void testTxValuesThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.values();
    }

    @Test
    public void testTxValuesSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.values();
        transaction.commitTransaction();
    }

    @Test
    public void testTxValuesWithPredicateThrowsExceptionWhenQuorumSizeNotMet() {
        TransactionContext transaction = getTransactionFromMinority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        expectedException.expect(QuorumException.class);
        map.values(TruePredicate.INSTANCE);
    }

    @Test
    public void testTxValuesWithPredicateSucceedsWhenQuorumSizeMet() {
        TransactionContext transaction = getTransactionFromMajority();
        TransactionalMap<Object, Object> map = getMap(transaction);

        map.values(TruePredicate.INSTANCE);
        transaction.commitTransaction();
    }

    private TransactionContext getTransactionFromMajority() {
        TransactionContext transactionContext = c1.newTransactionContext(options);
        transactionContext.beginTransaction();
        return transactionContext;
    }

    private TransactionContext getTransactionFromMinority() {
        TransactionContext transactionContext = c4.newTransactionContext(options);
        transactionContext.beginTransaction();
        return transactionContext;
    }

    private static TransactionalMap<Object, Object> getMap(TransactionContext transaction) {
        return transaction.getMap(randomMapName(MAP_NAME_PREFIX));
    }
}
