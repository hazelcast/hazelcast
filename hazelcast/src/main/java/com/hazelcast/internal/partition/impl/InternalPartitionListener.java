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
import com.hazelcast.internal.partition.PartitionReplicaChangeReason;
import com.hazelcast.internal.partition.operation.ClearReplicaOperation;
import com.hazelcast.internal.partition.operation.PromoteFromBackupOperation;
import com.hazelcast.internal.partition.operation.ResetReplicaVersionOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

/**
 * TODO: Javadoc Pending...
 *
 */
final class InternalPartitionListener implements PartitionListener {
    private final Node node;
    private final InternalPartitionServiceImpl partitionService;
    private final Address thisAddress;
    private final ILogger logger;
    private volatile PartitionListenerNode listenerHead;

    InternalPartitionListener(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.partitionService = partitionService;
        this.thisAddress = node.getThisAddress();
        logger = node.getLogger(InternalPartitionService.class);
    }

    @Override
    public void replicaChanged(PartitionReplicaChangeEvent event) {
        final int partitionId = event.getPartitionId();
        final int replicaIndex = event.getReplicaIndex();
        final Address newAddress = event.getNewAddress();
        final Address oldAddress = event.getOldAddress();
        final PartitionReplicaChangeReason reason = event.getReason();

        final boolean initialAssignment = event.getOldAddress() == null;

        if (replicaIndex > 0) {
            // backup replica owner changed!
            if (thisAddress.equals(oldAddress)) {
                clearPartition(partitionId, replicaIndex);
            } else if (thisAddress.equals(newAddress)) {
                synchronizePartition(partitionId, replicaIndex, reason, initialAssignment);
            }
        } else {
            if (!initialAssignment && thisAddress.equals(newAddress)) {
                // it is possible that I might become owner while waiting for sync request from the previous owner.
                // I should check whether if have failed to get backups from the owner and lost the partition for
                // some backups.
                promoteFromBackups(partitionId, reason, oldAddress);
            }
            partitionService.getReplicaManager().cancelReplicaSync(partitionId);
        }

        if (replicaIndex == 0 && newAddress == null && node.isRunning() && node.joined()) {
            logOwnerOfPartitionIsRemoved(event);
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

    private void clearPartition(final int partitionId, final int oldReplicaIndex) {
        NodeEngine nodeEngine = node.nodeEngine;
        ClearReplicaOperation op = new ClearReplicaOperation(oldReplicaIndex);
        op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(partitionService);
        nodeEngine.getOperationService().executeOperation(op);
    }

    private void synchronizePartition(int partitionId, int replicaIndex, PartitionReplicaChangeReason reason, boolean initialAssignment) {
        // if not initialized yet, no need to sync, since this is the initial partition assignment
        if (partitionService.getPartitionStateManager().isInitialized()) {
            long delayMillis = 0L;
            if (replicaIndex > 1) {
                // immediately trigger replica synchronization for the first backups
                // postpone replica synchronization for greater backups to a later time
                // high priority is 1st backups
                delayMillis = (long) (
                        InternalPartitionService.REPLICA_SYNC_RETRY_DELAY + (Math.random() * InternalPartitionService.DEFAULT_REPLICA_SYNC_DELAY));
            }

            resetReplicaVersion(partitionId, replicaIndex, reason, initialAssignment);
            partitionService.getReplicaManager().triggerPartitionReplicaSync(partitionId, replicaIndex, delayMillis);
        }
    }

    private void resetReplicaVersion(int partitionId, int replicaIndex, PartitionReplicaChangeReason reason, boolean initialAssignment) {
        NodeEngine nodeEngine = node.nodeEngine;
        ResetReplicaVersionOperation op = new ResetReplicaVersionOperation(reason, initialAssignment);
        op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setNodeEngine(nodeEngine).setService(partitionService);
        nodeEngine.getOperationService().executeOperation(op);
    }

    private void promoteFromBackups(int partitionId, PartitionReplicaChangeReason reason, Address oldAddress) {
        NodeEngine nodeEngine = node.nodeEngine;
        PromoteFromBackupOperation op = new PromoteFromBackupOperation(reason, oldAddress);
        op.setPartitionId(partitionId).setNodeEngine(nodeEngine).setService(partitionService);
        nodeEngine.getOperationService().executeOperation(op);
    }

    private void logOwnerOfPartitionIsRemoved(PartitionReplicaChangeEvent event) {
        String warning = "Owner of partition is being removed! " + "Possible data loss for partitionId=" + event.getPartitionId() + " , " + event;
        logger.warning(warning);
    }

    void addChildListener(PartitionListener listener) {
        PartitionListenerNode head = listenerHead;
        listenerHead = new PartitionListenerNode(listener, head);
    }
}
