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

package com.hazelcast.client.cluster;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static com.hazelcast.test.HazelcastTestSupport.warmUpPartitions;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientClusterStateTest {

    private TestHazelcastFactory factory;

    private HazelcastInstance[] instances;

    private HazelcastInstance instance;

    @Before
    public void before() {
        factory = new TestHazelcastFactory();
        instances = factory.newInstances(new Config(), 4);
        for (HazelcastInstance instance : instances) {
            assertClusterSizeEventually(4, instance);
        }
        instance = instances[instances.length - 1];
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testClient_canConnect_whenClusterState_frozen() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);
        factory.newHazelcastClient();
    }

    @Test
    public void testClient_canExecuteWriteOperations_whenClusterState_frozen() {
        warmUpPartitions(instances);

        changeClusterStateEventually(instance, ClusterState.FROZEN);
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        map.put(1, 1);
    }

    @Test
    public void testClient_canExecuteReadOperations_whenClusterState_frozen() {
        warmUpPartitions(instances);

        changeClusterStateEventually(instance, ClusterState.FROZEN);
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        map.get(1);
    }

    @Test
    public void testClient_canConnect_whenClusterState_passive() {
        instance.getCluster().changeClusterState(ClusterState.PASSIVE);
        factory.newHazelcastClient();
    }

    @Test(expected = IllegalStateException.class)
    public void testClient_canNotExecuteWriteOperations_whenClusterState_passive() {
        warmUpPartitions(instances);

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        changeClusterStateEventually(instance, ClusterState.PASSIVE);
        map.put(1, 1);
    }

    @Test
    public void testClient_canExecuteReadOperations_whenClusterState_passive() {
        warmUpPartitions(instances);

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        changeClusterStateEventually(instance, ClusterState.PASSIVE);
        map.get(1);
    }

    @Test
    public void testClient_canConnect_whenClusterState_goesBackToActive_fromPassive() {
        instance.getCluster().changeClusterState(ClusterState.PASSIVE);
        instance.getCluster().changeClusterState(ClusterState.ACTIVE);
        factory.newHazelcastClient();
    }

    @Test
    public void testClient_canExecuteOperations_whenClusterState_goesBackToActive_fromPassive() {
        warmUpPartitions(instances);

        changeClusterStateEventually(instance, ClusterState.PASSIVE);
        instance.getCluster().changeClusterState(ClusterState.ACTIVE);
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap(randomMapName());
        map.put(1, 1);
    }

    @Test
    public void testClusterShutdownDuringMapPutAll() {
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(10_000);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        final IMap<Object, Object> map = client.getMap(randomMapName());

        final HashMap values = new HashMap<Double, Double>();
        for (int i = 0; i < 1000; i++) {
            double value = Math.random();
            values.put(value, value);
        }

        final int numThreads = 10;
        final CountDownLatch threadsFinished = new CountDownLatch(numThreads);
        final CountDownLatch threadsStarted = new CountDownLatch(numThreads);

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < numThreads; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    ILogger logger = Logger.getLogger(getClass());
                    threadsStarted.countDown();
                    logger.info("putAll thread started");
                    while (true) {
                        try {
                            map.putAll(values);
                            Thread.sleep(100);
                        } catch (IllegalStateException e) {
                            logger.warning("Expected exception for Map putAll during cluster shutdown:", e);
                            break;
                        } catch (TargetDisconnectedException e) {
                            logger.warning("Expected exception for Map putAll during cluster shutdown:", e);
                            break;
                        } catch (InterruptedException e) {
                            // do nothing
                        }

                    }
                    threadsFinished.countDown();
                    logger.info("putAll thread finishing. Current finished thread count is:" + (numThreads - threadsFinished
                            .getCount()));
                }
            });
        }

        try {
            assertTrue("All threads could not be started", threadsStarted.await(1, TimeUnit.MINUTES));
        } catch (InterruptedException e) {
            fail("All threads could not be started due to InterruptedException. Could not start " + threadsStarted.getCount()
                    + " threads out of " + numThreads);
        }

        instance.getCluster().shutdown();

        executor.shutdown();

        try {
            assertTrue("All threads could not be finished", threadsFinished.await(2, TimeUnit.MINUTES));
        } catch (InterruptedException e) {
            fail("All threads could not be finished due to InterruptedException. Could not finish " + threadsFinished.getCount()
                    + " threads out of " + numThreads);
        } finally {
            executor.shutdownNow();
        }
    }
}
