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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.Future;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGE_FAILED;
import static com.hazelcast.core.LifecycleEvent.LifecycleState.MERGING;
import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * ClusterMergeTask prepares {@code Node}'s internal state and its services
 * to merge and then triggers join process to the new cluster.
 * It is triggered on every member in the cluster when the master member detects
 * another cluster to join which it thinks current cluster is split from.
 */
class ClusterMergeTask implements Runnable {

    private static final String MERGE_TASKS_EXECUTOR = "hz:cluster-merge";

    private final Node node;
    private final LifecycleServiceImpl lifecycleService;

    ClusterMergeTask(Node node) {
        this.node = node;
        this.lifecycleService = node.hazelcastInstance.getLifecycleService();
    }

    public void run() {
        lifecycleService.fireLifecycleEvent(MERGING);

        boolean joined = false;
        try {
            resetState();

            Collection<Runnable> coreTasks = collectMergeTasks(true);
            Collection<Runnable> nonCoreTasks = collectMergeTasks(false);

            resetServices();

            rejoin();

            joined = isJoined();

            if (joined) {
                try {
                    executeMergeTasks(coreTasks);
                    executeMergeTasks(nonCoreTasks);
                } finally {
                    disposeTasks(coreTasks, nonCoreTasks);
                }
            }

            ILogger logger = node.getLogger(getClass());
            if (logger.isFineEnabled()) {
                logger.fine("Finished merge tasks.");
            }
        } finally {
            lifecycleService.fireLifecycleEvent(joined ? MERGED : MERGE_FAILED);
        }
    }

    /**
     * Release associated task resources if tasks are {@link Disposable}
     */
    private void disposeTasks(Collection<Runnable>... tasks) {
        for (Collection<Runnable> task : tasks) {
            for (Runnable runnable : task) {
                if (runnable instanceof Disposable) {
                    ((Disposable) runnable).dispose();
                }
            }
        }
    }

    private boolean isJoined() {
        return node.isRunning() && node.getClusterService().isJoined();
    }

    private void resetState() {
        // reset node and membership state from now on this node won't be joined and won't have a master address
        node.reset();
        node.getClusterService().reset();
        node.getNodeExtension().getInternalHotRestartService().resetService(true);
        // stop the connection-manager:
        // - all socket connections will be closed
        // - connection listening thread will stop
        // - no new connection will be established
        node.getServer().stop();

        // clear waiting operations in queue and notify invocations to retry
        node.nodeEngine.reset();
    }

    private Collection<Runnable> collectMergeTasks(boolean coreServices) {
        // gather merge tasks from services
        Collection<SplitBrainHandlerService> services = node.nodeEngine.getServices(SplitBrainHandlerService.class);
        Collection<Runnable> tasks = new LinkedList<>();
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
        node.getServer().start();
        // re-join to the target cluster
        node.join();
    }

    private void executeMergeTasks(Collection<Runnable> tasks) {
        Collection<Future> futures = new LinkedList<>();

        for (Runnable task : tasks) {
            Future f = node.nodeEngine.getExecutionService().submit(MERGE_TASKS_EXECUTOR, task);
            futures.add(f);
        }

        for (Future f : futures) {
            try {
                waitOnFuture(f);
            } catch (HazelcastInstanceNotActiveException e) {
                ignore(e);
            } catch (Exception e) {
                node.getLogger(getClass()).severe("While merging...", e);
            }
        }
    }

    private <V> V waitOnFuture(Future<V> future) {
        try {
            return future.get();
        } catch (Throwable t) {
            if (!node.isRunning()) {
                future.cancel(true);
                throw new HazelcastInstanceNotActiveException();
            } else {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
}
