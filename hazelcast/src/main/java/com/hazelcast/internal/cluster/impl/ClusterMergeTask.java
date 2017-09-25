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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.instance.LifecycleServiceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.CoreService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGE_FAILED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;
import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * ClusterMergeTask prepares {@code Node}'s internal state and its services
 * to merge and then triggers join process to the new cluster.
 * It is triggered on every member in the cluster when the master member detects
 * another cluster to join which it thinks current cluster is split from.
 */
class ClusterMergeTask implements Runnable {

    private static final long MIN_WAIT_ON_FUTURE_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(10);
    private static final String MERGE_TASKS_EXECUTOR = "hz:cluster-merge";

    private final Node node;

    ClusterMergeTask(Node node) {
        this.node = node;
    }

    public void run() {
        LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
        lifecycleService.fireLifecycleEvent(MERGING);
        LifecycleState finalLifecycleState = MERGE_FAILED;

        try {
            resetState();

            Collection<Runnable> coreTasks = collectMergeTasks(true);

            Collection<Runnable> nonCoreTasks = collectMergeTasks(false);

            resetServices();

            rejoin();

            finalLifecycleState = getFinalLifecycleState();

            if (finalLifecycleState == MERGED) {
                executeMergeTasks(coreTasks);
                executeMergeTasks(nonCoreTasks);
            }
        } finally {
            lifecycleService.fireLifecycleEvent(finalLifecycleState);
        }
    }

    private LifecycleState getFinalLifecycleState() {
       return (node.isRunning() && node.getClusterService().isJoined()) ? MERGED : MERGE_FAILED;
    }

    private void resetState() {
        // reset node and membership state from now on this node won't be joined and won't have a master address
        node.reset();
        node.getClusterService().reset();
        // stop the connection-manager:
        // - all socket connections will be closed
        // - connection listening thread will stop
        // - no new connection will be established
        node.connectionManager.stop();

        // clear waiting operations in queue and notify invocations to retry
        node.nodeEngine.reset();
    }

    private Collection<Runnable> collectMergeTasks(boolean coreServices) {
        // gather merge tasks from services
        Collection<SplitBrainHandlerService> services = node.nodeEngine.getServices(SplitBrainHandlerService.class);
        Collection<Runnable> tasks = new LinkedList<Runnable>();
        for (SplitBrainHandlerService service : services) {
            if (coreServices != isCoreService(service)) {
                continue;
            }
            Runnable runnable = service.prepareMergeRunnable();
            if (runnable != null) {
                tasks.add(runnable);
            }
        }
        return tasks;
    }

    private boolean isCoreService(SplitBrainHandlerService service) {
        return service instanceof CoreService;
    }

    private void resetServices() {
        // reset all services to their initial state
        Collection<ManagedService> managedServices = node.nodeEngine.getServices(ManagedService.class);
        for (ManagedService service : managedServices) {
            if (service instanceof ClusterService) {
                // ClusterService is already reset in resetState()
                continue;
            }
            service.reset();
        }
    }

    private void rejoin() {
        // start connection-manager to setup and accept new connections
        node.connectionManager.start();
        // re-join to the target cluster
        node.join();
    }

    private void executeMergeTasks(Collection<Runnable> tasks) {
        // execute merge tasks
        Collection<Future> futures = new LinkedList<Future>();
        for (Runnable task : tasks) {
            Future f = node.nodeEngine.getExecutionService().submit(MERGE_TASKS_EXECUTOR, task);
            futures.add(f);
        }
        long callTimeoutMillis = node.getProperties().getMillis(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS);
        for (Future f : futures) {
            try {
                waitOnFutureInterruptible(f, callTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (HazelcastInstanceNotActiveException e) {
                EmptyStatement.ignore(e);
            } catch (Exception e) {
                node.getLogger(getClass()).severe("While merging...", e);
            }
        }
    }

    private <V> V waitOnFutureInterruptible(Future<V> future, long timeout, TimeUnit timeUnit)
            throws ExecutionException, InterruptedException, TimeoutException {

        isNotNull(timeUnit, "timeUnit");
        long totalTimeoutMs = timeUnit.toMillis(timeout);
        while (true) {
            long timeoutStepMs = Math.min(MIN_WAIT_ON_FUTURE_TIMEOUT_MILLIS, totalTimeoutMs);
            try {
                return future.get(timeoutStepMs, TimeUnit.MILLISECONDS);
            } catch (TimeoutException t) {
                totalTimeoutMs -= timeoutStepMs;
                if (totalTimeoutMs <= 0) {
                    throw t;
                }
                if (!node.isRunning()) {
                    future.cancel(true);
                    throw new HazelcastInstanceNotActiveException();
                }
            }
        }
    }
}
