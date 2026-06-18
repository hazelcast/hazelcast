/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.logging.Level;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;

/**
 * Clears expired records.
 */
public class MapClearExpiredOperation extends AbstractMapLocalOperation
        implements PartitionAwareOperation, MutatingOperation {

    private CleanupState cleanupState;

    public MapClearExpiredOperation() {
    }

    private MapClearExpiredOperation(String mapName, CleanupState cleanupState) {
        super(mapName);
        this.cleanupState = cleanupState;
        createRecordStoreOnDemand = false;
    }

    public static Operation newOperation(NodeEngine nodeEngine, int partitionId,
                                         Queue<String> mapNames, int expirationPercentage) {
        assert !mapNames.isEmpty();
        return newOperation(nodeEngine, partitionId, new CleanupState(mapNames, expirationPercentage));
    }

    private static Operation newOperation(NodeEngine nodeEngine, int partitionId, CleanupState cleanupState) {
        assert cleanupState.hasNextMap();

        return new MapClearExpiredOperation(cleanupState.nextMapName(), cleanupState)
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setServiceName(SERVICE_NAME)
                .setOperationResponseHandler(createEmptyResponseHandler());
    }

    @Override
    protected void runInternal() {
        if (getNodeEngine().getLocalMember().isLiteMember()) {
            // this operation shouldn't run on lite members. This situation can potentially be seen
            // when converting a data-member to lite-member during merge operations.
            return;
        }

        if (recordStore != null && recordStore.isExpirable()) {
            recordStore.evictExpiredEntries(cleanupState.expirationPercentage,
                    Clock.currentTimeMillis(), !isOwner());
        }
    }

    private boolean isOwner() {
        final NodeEngine nodeEngine = getNodeEngine();
        final Address owner = nodeEngine.getPartitionService().getPartitionOwner(getPartitionId());
        return nodeEngine.getThisAddress().equals(owner);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        try {
            super.onExecutionFailure(e);
        } finally {
            prepareForNextCleanup();
        }
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof PartitionMigratingException) {
            ILogger logger = getLogger();
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, e.toString());
            }
        } else {
            super.logError(e);
        }
    }

    @Override
    public void afterRunInternal() {
        NodeEngine nodeEngine = getNodeEngine();
        if (nodeEngine.getLocalMember().isLiteMember() || !cleanupState.hasNextMap()) {
            prepareForNextCleanup();
            return;
        }

        nodeEngine.getOperationService().execute(newOperation(
                nodeEngine, getPartitionId(), cleanupState));
    }

    protected void prepareForNextCleanup() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        partitionContainer.setHasRunningCleanup(false);
        partitionContainer.setLastCleanupTime(Clock.currentTimeMillis());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", expirationPercentage=").append(cleanupState.expirationPercentage)
                .append(", remainingMapCount=").append(cleanupState.remainingMapCount());
    }

    private static final class CleanupState {
        private final Deque<String> pendingMapNames;
        private final int expirationPercentage;

        private CleanupState(Queue<String> mapNames, int expirationPercentage) {
            this.pendingMapNames = new ArrayDeque<>(mapNames);
            this.expirationPercentage = expirationPercentage;
        }

        private boolean hasNextMap() {
            return !pendingMapNames.isEmpty();
        }

        private String nextMapName() {
            return pendingMapNames.removeFirst();
        }

        private int remainingMapCount() {
            return pendingMapNames.size();
        }
    }
}
