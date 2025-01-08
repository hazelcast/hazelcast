/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

public class AddIndexBackupOperation extends MapOperation implements BackupOperation,
        // AddIndexBackupOperation is used when map proxy for IMap with indexes is initialized during passive state
        // (e.g. IMap is read for the first time after HotRestart recovery when the cluster is still in PASSIVE state)
        AllowedDuringPassiveState {

    private IndexConfig config;

    public AddIndexBackupOperation() {
    }

    public AddIndexBackupOperation(String name, IndexConfig config) {
        super(name);
        this.config = config;
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void runInternal() {
        int partitionId = getPartitionId();

        IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(partitionId);
        indexRegistry.recordIndexDefinition(config);

        // Register index also in backup operation. This usually should be redundant
        // as usually the member should be also owner of some partitions. But just in case it is not,
        // we also register the index here.
        // It would be better to do once on each member instead of for each partition
        // but currently there is no appropriate operation for that as index must be registered on each member.
        mapServiceContext.registerIndex(name, config);
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        config = in.readObject();
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ADD_INDEX_BACKUP;
    }
}
