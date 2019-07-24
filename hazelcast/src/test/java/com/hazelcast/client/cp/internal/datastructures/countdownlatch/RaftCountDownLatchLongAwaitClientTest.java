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

package com.hazelcast.client.cp.internal.datastructures.countdownlatch;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftCountDownLatchLongAwaitClientTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private String objectName = "latch";
    private String proxyName = objectName + "@group1";
    private int groupSize = 3;
    private CPGroupId groupId;
    private final long callTimeoutSeconds = 15;
    private HazelcastInstance client;

    @Override
    protected TestHazelcastInstanceFactory createTestFactory() {
        return new TestHazelcastFactory();
    }

    @Before
    public void setup() {
        instances = newInstances(groupSize);
        TestHazelcastFactory f = (TestHazelcastFactory) factory;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), String.valueOf(callTimeoutSeconds));
        client = f.newHazelcastClient(clientConfig);
        groupId = ((RaftCountDownLatchProxy) client.getCPSubsystem().getCountDownLatch(proxyName)).getGroupId();
    }

    @Test
    public void when_awaitDurationIsLongerThanOperationTimeout_then_invocationFromClientWaits() throws ExecutionException, InterruptedException {
        ICountDownLatch latch = client.getCPSubsystem().getCountDownLatch(proxyName);
        HazelcastInstance instance = getLeaderInstance(instances, groupId);
        latch.trySetCount(1);

        Future<Boolean> f = spawn(() -> latch.await(5, TimeUnit.MINUTES));

        assertTrueEventually(() -> {
            RaftCountDownLatchService service = getNodeEngineImpl(instance).getService(RaftCountDownLatchService.SERVICE_NAME);
            assertFalse(service.getLiveOperations(groupId).isEmpty());
        });

        assertTrueAllTheTime(() -> {
            RaftCountDownLatchService service = getNodeEngineImpl(instance).getService(RaftCountDownLatchService.SERVICE_NAME);
            assertFalse(service.getLiveOperations(groupId).isEmpty());
        }, callTimeoutSeconds + 5);

        latch.countDown();

        assertCompletesEventually(f);
        assertTrue(f.get());
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        String callTimeoutStr = String.valueOf(SECONDS.toMillis(callTimeoutSeconds));
        return super.createConfig(cpNodeCount, groupSize)
                    .setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), callTimeoutStr);
    }

}
