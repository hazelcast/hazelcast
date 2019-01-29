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

package com.hazelcast.quorum.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.quorum.AbstractQuorumTest;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
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
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class TransactionalMapQuorumWriteTest extends AbstractQuorumTest {

    @Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {

        TransactionOptions onePhaseOption = TransactionOptions.getDefault();
        onePhaseOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        return Arrays.asList(
                new Object[]{onePhaseOption, WRITE},
                new Object[]{twoPhaseOption, WRITE},
                new Object[]{onePhaseOption, READ_WRITE},
                new Object[]{twoPhaseOption, READ_WRITE}
        );
    }

    @Parameter(0)
    public TransactionOptions options;

    @Parameter(1)
    public static QuorumType quorumType;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(new Config(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void txPut_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.put("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txPut_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.put("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test
    public void txGetForUpdate_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.getForUpdate("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txGetForUpdate_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.getForUpdate("foo");
        transactionContext.commitTransaction();
    }

    @Test
    public void txRemove_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.remove("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txRemove_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.remove("foo");
        transactionContext.commitTransaction();
    }

    @Test
    public void txRemoveValue_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.remove("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txRemoveValue_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.remove("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test
    public void txDelete_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.delete("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txDelete_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.delete("foo");
        transactionContext.commitTransaction();
    }

    @Test
    public void txSet_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.set("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txSet_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.set("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test
    public void txPutTtl_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.put("foo", "bar", 10, TimeUnit.SECONDS);
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txPutTtl_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.put("foo", "bar", 10, TimeUnit.SECONDS);
        transactionContext.commitTransaction();
    }

    @Test
    public void txPutIfAbsent_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.putIfAbsent("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txPutIfAbsent_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.putIfAbsent("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test
    public void txReplace_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.replace("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txReplace_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.replace("foo", "bar");
        transactionContext.commitTransaction();
    }

    @Test
    public void txReplaceValue_successful_whenQuorumSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.replace("foo", "bar", "baz");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txReplaceValue_failing_whenQuorumSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + quorumType.name());
        map.replace("foo", "bar", "baz");
        transactionContext.commitTransaction();
    }

    public TransactionContext newTransactionContext(int index) {
        return cluster.instance[index].newTransactionContext(options);
    }
}
