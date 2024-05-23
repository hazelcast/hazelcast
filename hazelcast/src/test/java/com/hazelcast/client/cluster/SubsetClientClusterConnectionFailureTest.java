/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cluster;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SubsetClientClusterConnectionFailureTest extends ClientTestSupport {

    private TestHazelcastFactory factory;

    @Before
    public void before() throws IOException {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testClientConnectionFailsWithException_whenNoSubsetGroupsReturned() {
        // create cluster with 2 partition groups
        factory.newInstances(newConfigWithAttribute("A"), 1);
        factory.newInstances(newConfigWithAttribute("B"), 1);

        // ensure cluster is formed
        assertClusterSizeEventually(2, factory.getAllHazelcastInstances());
        waitAllForSafeState(factory.getAllHazelcastInstances());

        // create a subset client config
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().getSubsetRoutingConfig().setEnabled(true);

        // Check that we get an UnsupportedOperationException when trying to create client
        // with subset routing enabled and no member group defined
        assertThrows(InvalidConfigurationException.class, () -> factory.newHazelcastClient(clientConfig));
        assertTrueEventually(() -> factory.getAllHazelcastInstances().forEach(
                hazelcastInstance -> {
                    assertEquals(0, hazelcastInstance.getClientService().getConnectedClients().size());
                }
        ));
    }

    @Test
    public void testClientConnectionWithAsyncStart_FailsAndShutsDown_whenNoSubsetGroupsReturned() {
        // create cluster with 2 partition groups
        factory.newInstances(newConfigWithAttribute("A"), 1);
        factory.newInstances(newConfigWithAttribute("B"), 1);

        // ensure cluster is formed
        assertClusterSizeEventually(2, factory.getAllHazelcastInstances());
        waitAllForSafeState(factory.getAllHazelcastInstances());

        // create a subset client with async start config
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().getSubsetRoutingConfig().setEnabled(true);
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        assertTrueEventually(() -> assertFalse(client.getLifecycleService().isRunning()));
        factory.getAllHazelcastInstances().forEach(
                hazelcastInstance -> {
                    assertEquals(0, hazelcastInstance.getClientService().getConnectedClients().size());
                }
        );
    }

    @Test
    public void testFastFailureOnConnectionWithAsyncStart_whenClientCreatedBeforeCluster() {
        // create a subset client with async start config
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().getSubsetRoutingConfig().setEnabled(true);
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        // create cluster with 2 partition groups
        factory.newInstances(newConfigWithAttribute("A"), 1);
        factory.newInstances(newConfigWithAttribute("B"), 1);

        // ensure cluster is formed
        assertClusterSizeEventually(2, factory.getAllHazelcastInstances());
        waitAllForSafeState(factory.getAllHazelcastInstances());

        assertTrueEventually(() -> assertFalse(client.getLifecycleService().isRunning()));
        factory.getAllHazelcastInstances().forEach(
                hazelcastInstance -> {
                    assertEquals(0, hazelcastInstance.getClientService().getConnectedClients().size());
                }
        );
    }

    @Test
    public void testFastFailure_whenClientCreatedBeforeCluster() throws InterruptedException {
        // create a subset client with async start config
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        clientConfig.getNetworkConfig().getSubsetRoutingConfig().setEnabled(true);

        Future createClientFuture = null;
        try {
            createClientFuture = spawn(() -> {
                factory.newHazelcastClient(clientConfig);
            });

            // create cluster with 2 partition groups
            factory.newInstances(newConfigWithAttribute("A"), 1);
            factory.newInstances(newConfigWithAttribute("B"), 1);

            // ensure cluster is formed
            assertClusterSizeEventually(2, factory.getAllHazelcastInstances());
            waitAllForSafeState(factory.getAllHazelcastInstances());

            Future finalCreateClientFuture = createClientFuture;

            // indicates the client is either connected or shutdown.
            assertTrueEventually(() -> assertTrue(finalCreateClientFuture.isDone()));

            factory.getAllHazelcastInstances().forEach(
                    hazelcastInstance -> {
                        assertEquals(0, hazelcastInstance.getClientService().getConnectedClients().size());
                    }
            );
        } finally {
            createClientFuture.cancel(true);
        }
    }

    @NotNull
    private Config newConfigWithAttribute(String attribute) {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        configureNodeAware(config);
        config.setProperty("hazelcast.client.internal.push.period.seconds", "2");
        config.getMemberAttributeConfig()
                .setAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE, attribute);
        return config;
    }

    private void configureNodeAware(Config config) {
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        partitionGroupConfig
                .setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.NODE_AWARE);
    }
}
