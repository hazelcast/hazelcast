/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.splitbrainprotection.map;

import com.hazelcast.query.Predicates;
import com.hazelcast.splitbrainprotection.AbstractSplitBrainProtectionTest;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
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

import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionalMapSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "Executing: {0} {1}")
    public static Collection<Object[]> parameters() {

        TransactionOptions onePhaseOption = TransactionOptions.getDefault();
        onePhaseOption.setTransactionType(ONE_PHASE);

        TransactionOptions twoPhaseOption = TransactionOptions.getDefault();
        twoPhaseOption.setTransactionType(TWO_PHASE);

        return Arrays.asList(
                new Object[]{onePhaseOption, READ},
                new Object[]{twoPhaseOption, READ},
                new Object[]{onePhaseOption, READ_WRITE},
                new Object[]{twoPhaseOption, READ_WRITE}
        );
    }

    @Parameter(0)
    public static TransactionOptions options;

    @Parameter(1)
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void txGet_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.get("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txGet_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.get("foo");
        transactionContext.commitTransaction();
    }

    @Test
    public void txSize_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.size();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txSize_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.size();
        transactionContext.commitTransaction();
    }

    @Test
    public void txContainsKey_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.containsKey("foo");
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txContainsKey_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.containsKey("foo");
        transactionContext.commitTransaction();
    }

    @Test
    public void txIsEmpty_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.isEmpty();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txIsEmpty_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.isEmpty();
        transactionContext.commitTransaction();
    }

    @Test
    public void txKeySet_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.keySet();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txKeySet_failing_whenSplitBrainProtectionSize_notNet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.keySet();
        transactionContext.commitTransaction();
    }

    @Test
    public void txKeySetPredicate_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.keySet(Predicates.alwaysTrue());
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txKeySetPredicate_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.keySet(Predicates.alwaysTrue());
        transactionContext.commitTransaction();
    }

    @Test
    public void txValues_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.values();
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txValues_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.values();
        transactionContext.commitTransaction();
    }

    @Test
    public void txValuesPredicate_successful_whenSplitBrainProtectionSize_met() {
        TransactionContext transactionContext = newTransactionContext(0);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.values(Predicates.alwaysTrue());
        transactionContext.commitTransaction();
    }

    @Test(expected = TransactionException.class)
    public void txValuesPredicate_failing_whenSplitBrainProtectionSize_notMet() {
        TransactionContext transactionContext = newTransactionContext(3);
        transactionContext.beginTransaction();
        TransactionalMap<Object, Object> map = transactionContext.getMap(MAP_NAME + splitBrainProtectionOn.name());
        map.values(Predicates.alwaysTrue());
        transactionContext.commitTransaction();
    }

    public TransactionContext newTransactionContext(int index) {
        return cluster.instance[index].newTransactionContext(options);
    }
}
