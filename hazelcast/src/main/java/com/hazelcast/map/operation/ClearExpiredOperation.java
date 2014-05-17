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
import com.hazelcast.map.eviction.EvictionHelper;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.List;

/**
 * Clear expired records.
 */
public class ClearExpiredOperation extends AbstractMapOperation implements PartitionAwareOperation {

    private List expiredKeyValueSequence;

    public ClearExpiredOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        final PartitionContainer partitionContainer = mapService.getPartitionContainer(getPartitionId());
        // this should be existing record store since we don't want to trigger record store creation.
        final RecordStore recordStore = partitionContainer.getExistingRecordStore(name);
        if (recordStore == null) {
            return;
        }
        expiredKeyValueSequence = recordStore.findUnlockedExpiredRecords();
    }

    @Override
    public void afterRun() throws Exception {
        final List expiredKeyValueSequence = this.expiredKeyValueSequence;
        if (expiredKeyValueSequence == null || expiredKeyValueSequence.isEmpty()) {
            return;
        }
        final MapService mapService = this.mapService;
        final String mapName = this.name;
        final NodeEngine nodeEngine = getNodeEngine();
        final Address owner = nodeEngine.getPartitionService().getPartitionOwner(getPartitionId());
        final boolean isOwner = nodeEngine.getThisAddress().equals(owner);
        final int size = expiredKeyValueSequence.size();
        for (int i = 0; i < size; i += 2) {
            Data key = (Data) expiredKeyValueSequence.get(i);
            Object value = expiredKeyValueSequence.get(i + 1);
            mapService.interceptAfterRemove(mapName, value);
            if (mapService.isNearCacheAndInvalidationEnabled(mapName)) {
                mapService.invalidateAllNearCaches(mapName, key);
            }
            if (isOwner) {
                EvictionHelper.fireEvent(key, value, mapName, mapService);
            }
        }
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
