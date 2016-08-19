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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Clear expired records.
 */
public class ClearExpiredOperation extends Operation implements PartitionAwareOperation, MutatingOperation {

    private int expirationPercentage;

    public ClearExpiredOperation(int expirationPercentage) {
        this.expirationPercentage = expirationPercentage;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        final MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        final ConcurrentMap<String, RecordStore> recordStores = partitionContainer.getMaps();
        final boolean backup = !isOwner();
        for (final RecordStore recordStore : recordStores.values()) {
            if (recordStore.size() > 0 && recordStore.isExpirable()) {
                recordStore.evictExpiredEntries(expirationPercentage, backup);
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
    public void afterRun() throws Exception {
        final MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        partitionContainer.setHasRunningCleanup(false);
        partitionContainer.setLastCleanupTime(Clock.currentTimeMillis());
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", expirationPercentage=").append(expirationPercentage);
    }
}
