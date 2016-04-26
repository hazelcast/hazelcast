/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.logging.ILogger;

/**
 * PartitionListener used to listen partition change events internally.
 * Most significant responsibility of this listener is to increment
 * partition-state version on each change.
 * <p>
 * Also this listener delegates the partition change events to its child
 * listeners.
 */
final class InternalPartitionListener implements PartitionListener {
    private final Node node;
    private final InternalPartitionServiceImpl partitionService;
    private final ILogger logger;
    private volatile PartitionListenerNode listenerHead;

    InternalPartitionListener(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.partitionService = partitionService;
        logger = node.getLogger(InternalPartitionService.class);
    }

    @Override
    public void replicaChanged(PartitionReplicaChangeEvent event) {
        final int partitionId = event.getPartitionId();
        final int replicaIndex = event.getReplicaIndex();

        if (replicaIndex == 0) {
            partitionService.getReplicaManager().cancelReplicaSync(partitionId);
        }

        if (node.isMaster()) {
            partitionService.getPartitionStateManager().incrementVersion();
        }

        callListeners(event);
    }

    private void callListeners(PartitionReplicaChangeEvent event) {
        PartitionListenerNode listenerNode = listenerHead;
        while (listenerNode != null) {
            try {
                listenerNode.listener.replicaChanged(event);
            } catch (Throwable e) {
                logger.warning("While calling PartitionListener: " + listenerNode.listener, e);
            }
            listenerNode = listenerNode.next;
        }
    }

    void addChildListener(PartitionListener listener) {
        PartitionListenerNode head = listenerHead;
        listenerHead = new PartitionListenerNode(listener, head);
    }

    private static final class PartitionListenerNode {
        final PartitionListener listener;
        final PartitionListenerNode next;

        PartitionListenerNode(PartitionListener listener, PartitionListenerNode next) {
            this.listener = listener;
            this.next = next;
        }
    }
}
