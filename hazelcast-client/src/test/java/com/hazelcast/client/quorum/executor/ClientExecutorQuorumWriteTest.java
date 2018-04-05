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

package com.hazelcast.client.quorum.executor;

import com.hazelcast.client.quorum.PartitionedClusterClients;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.quorum.executor.ExecutorQuorumWriteTest;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientExecutorQuorumWriteTest extends ExecutorQuorumWriteTest {

    private static PartitionedClusterClients clients;

    @BeforeClass
    public static void setUp() {
        TestHazelcastFactory factory = new TestHazelcastFactory();
        initTestEnvironment(new Config(), factory);
        clients = new PartitionedClusterClients(cluster, factory);
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
        clients.terminateAll();
    }

    @Override
    protected IExecutorService exec(int index, QuorumType quorumType) {
        return exec(index, quorumType, "");
    }

    @Override
    protected IExecutorService exec(int index, QuorumType quorumType, String postfix) {
        return clients.client(index).getExecutorService(EXEC_NAME + quorumType.name() + postfix);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_quorum_short_timeout() throws Exception {
        super.invokeAll_timeout_quorum_short_timeout();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_quorum_long_timeout() throws Exception {
        super.invokeAll_timeout_quorum_long_timeout();
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void invokeAll_timeout_noQuorum() throws Exception {
        super.invokeAll_timeout_noQuorum();
    }
}
