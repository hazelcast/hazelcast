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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.service.TestGetOperation;
import com.hazelcast.internal.partition.service.TestPutOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class AbstractGracefulShutdownCorrectnessTest extends PartitionCorrectnessTestSupport {

    @Parameterized.Parameter(2)
    public int shutdownNodeCount;

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenNodesShutdown() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        startNodes(config, nodeCount);
        warmUpPartitions(factory.getAllHazelcastInstances());

        fillData(hz);
        assertSizeAndDataEventually();

        shutdownNodes(shutdownNodeCount);

        assertSizeAndDataEventually();
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenNodesStartedShutdown() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);
        assertSizeAndDataEventually();

        int size = 1;
        while (size < (nodeCount + 1)) {
            startNodes(config, shutdownNodeCount + 1);
            size += (shutdownNodeCount + 1);

            assertSizeAndDataEventually();

            shutdownNodes(shutdownNodeCount);
            size -= shutdownNodeCount;

            assertSizeAndDataEventually();
        }
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenNodesStartedShutdown_withRestart() throws InterruptedException {
        Config config = getConfig(true, false);

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        fillData(hz);
        assertSizeAndDataEventually();

        Collection<Address> addresses = Collections.emptySet();

        int size = 1;
        while (size < (nodeCount + 1)) {
            int startCount = (shutdownNodeCount + 1) - addresses.size();
            startNodes(config, addresses);
            startNodes(config, startCount);
            size += (shutdownNodeCount + 1);

            assertSizeAndDataEventually();

            addresses = shutdownNodes(shutdownNodeCount);
            size -= shutdownNodeCount;

            assertSizeAndDataEventually();
        }
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenNodesStartedShutdown_whileOperationsOngoing() throws InterruptedException {
        final Config config = getConfig(true, false);

        Future future = spawn(() -> {
            LinkedList<HazelcastInstance> instances
                    = new LinkedList<>(Arrays.asList(factory.newInstances(config, nodeCount)));
            try {
                for (int i = 0; i < 3; i++) {
                    shutdownNodes(instances, shutdownNodeCount);
                    Collection<HazelcastInstance> startedInstances = startNodes(config, shutdownNodeCount);
                    instances.addAll(startedInstances);
                }
                shutdownNodes(instances, shutdownNodeCount);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        HazelcastInstance hz = factory.newHazelcastInstance(config);
        NodeEngine nodeEngine = getNodeEngineImpl(hz);

        while (!nodeEngine.getClusterService().isJoined()) {
            TimeUnit.MILLISECONDS.sleep(100);
        }

        OperationService operationService = nodeEngine.getOperationService();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();

        int value = 0;
        do {
            value++;
            for (int p = 0; p < partitionCount; p++) {
                operationService.invokeOnPartition(null, new TestPutOperation(value), p).join();
            }
        } while (!future.isDone());

        for (int p = 0; p < partitionCount; p++) {
            Integer actual = (Integer) operationService.invokeOnPartition(null, new TestGetOperation(), p).join();
            assertNotNull(actual);
            assertEquals(value, actual.intValue());
        }
    }

    private Collection<Address> shutdownNodes(int count) throws InterruptedException {
        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        List<HazelcastInstance> instanceList = instances instanceof List
                ? (List<HazelcastInstance>) instances
                : new LinkedList<HazelcastInstance>(instances);

        return shutdownNodes(instanceList, count);
    }

    private Collection<Address> shutdownNodes(List<HazelcastInstance> instances, int count) throws InterruptedException {
        assertThat(instances.size(), greaterThanOrEqualTo(count));

        if (count == 1) {
            HazelcastInstance hz = instances.remove(0);
            Address address = getNode(hz).getThisAddress();
            hz.shutdown();
            return Collections.singleton(address);
        } else {
            final CountDownLatch latch = new CountDownLatch(count);
            Collection<Address> addresses = new HashSet<Address>();

            for (int i = 0; i < count; i++) {
                final HazelcastInstance hz = instances.remove(0);
                addresses.add(getNode(hz).getThisAddress());

                new Thread() {
                    public void run() {
                        hz.shutdown();
                        latch.countDown();
                    }
                }.start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
            return addresses;
        }
    }
}
