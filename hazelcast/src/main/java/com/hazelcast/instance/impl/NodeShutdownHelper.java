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

package com.hazelcast.instance.impl;

import com.hazelcast.core.LifecycleEvent;

/**
 * Helper methods for node shutdown scenarios.
 */
public final class NodeShutdownHelper {

    private NodeShutdownHelper() {
    }

    /**
     * Shutdowns a node by firing lifecycle events. Do not call this method for every node shutdown scenario
     * since {@link com.hazelcast.core.LifecycleListener}s will end up more than one
     * {@link com.hazelcast.core.LifecycleEvent.LifecycleState#SHUTTING_DOWN}
     * or {@link com.hazelcast.core.LifecycleEvent.LifecycleState#SHUTDOWN} events.
     *
     * @param node      Node to shutdown.
     * @param terminate <code>false</code> for graceful shutdown, <code>true</code> for terminate (un-graceful shutdown)
     */
    public static void shutdownNodeByFiringEvents(Node node, boolean terminate) {
        final HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
        final LifecycleServiceImpl lifecycleService = hazelcastInstance.getLifecycleService();
        lifecycleService.fireLifecycleEvent(LifecycleEvent.LifecycleState.SHUTTING_DOWN);
        node.shutdown(terminate);
        lifecycleService.fireLifecycleEvent(LifecycleEvent.LifecycleState.SHUTDOWN);
    }
}
