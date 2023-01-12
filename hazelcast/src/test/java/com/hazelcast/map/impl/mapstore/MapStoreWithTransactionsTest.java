/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class MapStoreWithTransactionsTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "offload: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {
                {true},
                {false}
        });
    }

    @Parameterized.Parameter
    public boolean offload;

    private static final String MAP_NAME = "map-with-map-store";
    private static final int KEY_COUNT = 1_001;

    @Override
    public Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getMapConfig(MAP_NAME)
                .getMapStoreConfig()
                .setEnabled(true)
                .setOffload(offload)
                .setImplementation(new MapStoreAdapter());
        return config;
    }

    @Test
    public void lock_count_is_same_with_before_after_txn_commits() {
        HazelcastInstance node = createHazelcastInstance();

        IMap map = node.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            map.set(i, i);
        }

        for (int i = 0; i < KEY_COUNT; i++) {
            map.lock(i);
        }

        TransactionContext context = newTransactionContext(node, TWO_PHASE);
        context.beginTransaction();

        TransactionalMap txnMap = context.getMap(MAP_NAME);

        for (int i = 0; i < KEY_COUNT; i++) {
            txnMap.set(i, i * 2);
        }

        context.commitTransaction();

        for (int i = 0; i < KEY_COUNT; i++) {
            assertTrue(map.isLocked(i));
        }

    }

    private static TransactionContext newTransactionContext(HazelcastInstance node,
                                                            TransactionOptions.TransactionType twoPhase) {
        TransactionOptions options = new TransactionOptions()
                .setTransactionType(twoPhase);
        return node.newTransactionContext(options);
    }
}
