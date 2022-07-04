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

package com.hazelcast.map.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

/**
 * Clears expired records.
 */
public class MapClearExpiredOperation extends AbstractLocalOperation
        implements PartitionAwareOperation, MutatingOperation {

    private int expirationPercentage;

    public MapClearExpiredOperation(int expirationPercentage) {
        this.expirationPercentage = expirationPercentage;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        if (getNodeEngine().getLocalMember().isLiteMember()) {
            // this operation shouldn't run on lite members. This situation can potentially be seen
            // when converting a data-member to lite-member during merge operations.
            return;
        }

        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        ConcurrentMap<String, RecordStore> recordStores = partitionContainer.getMaps();
        boolean backup = !isOwner();
        long now = Clock.currentTimeMillis();
        for (RecordStore recordStore : recordStores.values()) {
            if (recordStore.isExpirable()) {
                recordStore.evictExpiredEntries(expirationPercentage, now, backup);
                recordStore.disposeDeferredBlocks();
            }
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
    public void afterRun() throws Exception {
        prepareForNextCleanup();
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

        sb.append(", expirationPercentage=").append(expirationPercentage);
    }
}
