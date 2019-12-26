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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDistributedObjectListenerTest
        extends com.hazelcast.core.DistributedObjectListenerTest {

    @Test
    public void distributedObjectsCreatedBack_whenClusterRestart_withSingleNode() {
        final HazelcastInstance server = getRandomServer();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(clientConfig);

        instance.getMap("test");

        assertTrueEventually(() -> {
            Collection<HazelcastInstance> servers = hazelcastFactory.getAllHazelcastInstances();
            for (HazelcastInstance server1 : servers) {
                Collection<DistributedObject> distributedObjects = server1.getDistributedObjects();
                assertEquals(1, distributedObjects.size());
            }
        });

        hazelcastFactory.shutdownAllMembers();

        final HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(() -> {
            Collection<DistributedObject> distributedObjects = instance2.getDistributedObjects();
            assertEquals(1, distributedObjects.size());
        });
    }

    @Test
    public void distributedObjectsCreatedBack_whenClusterRestart_withMultipleNode() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance instance = hazelcastFactory.newHazelcastClient(clientConfig);

        instance.getMap("test");

        hazelcastFactory.shutdownAllMembers();

        final HazelcastInstance newClusterInstance1 = hazelcastFactory.newHazelcastInstance();
        final HazelcastInstance newClusterInstance2 = hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                Collection<DistributedObject> distributedObjects1 = newClusterInstance1.getDistributedObjects();
                assertEquals(1, distributedObjects1.size());
                Collection<DistributedObject> distributedObjects2 = newClusterInstance2.getDistributedObjects();
                assertEquals(1, distributedObjects2.size());
            }
        });
    }

}
