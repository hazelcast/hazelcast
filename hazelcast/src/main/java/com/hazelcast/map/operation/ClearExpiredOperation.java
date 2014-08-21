/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.operation;

import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.mapstore.MapDataStore;
import com.hazelcast.map.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Clear expired records.
 */
public class ClearExpiredOperation extends AbstractOperation implements PartitionAwareOperation {

    private int expirationPercentage;

    public ClearExpiredOperation(int expirationPercentage) {
        this.expirationPercentage = expirationPercentage;
    }

    @Override
    public void run() throws Exception {
        final long now = Clock.currentTimeMillis();
        final MapService mapService = getService();
        final PartitionContainer partitionContainer = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
        final ConcurrentMap<String, RecordStore> recordStores = partitionContainer.getMaps();
        final boolean backup = !isOwner();
        for (final RecordStore recordStore : recordStores.values()) {
            if (recordStore.size() > 0 && recordStore.isExpirable()) {
                recordStore.evictExpiredEntries(expirationPercentage, backup);
            }
            cleanupEvictionStagingArea(recordStore, now);
        }
    }

    private void cleanupEvictionStagingArea(RecordStore recordStore, long now) {
        if (recordStore == null) {
            return;
        }
        final MapDataStore<Data, Object> mapDataStore = recordStore.getMapDataStore();
        if (mapDataStore instanceof WriteBehindStore) {
            ((WriteBehindStore) mapDataStore).cleanupStagingArea(now);
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
        final PartitionContainer partitionContainer = mapService.getMapServiceContext().getPartitionContainer(getPartitionId());
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
    public String toString() {
        return "ClearExpiredOperation{}";
    }
}
