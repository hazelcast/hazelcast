/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

/**
 * Operation which evicts all keys except locked ones.
 */
public class EvictAllBackupOperation extends AbstractMapOperation implements BackupOperation, MutatingOperation,
        DataSerializable {

    public EvictAllBackupOperation() {
    }

    public EvictAllBackupOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final RecordStore recordStore = mapServiceContext.getExistingRecordStore(getPartitionId(), name);
        //if there is no recordStore, then there is nothing to evict.
        if (recordStore == null) {
            return;
        }
        recordStore.evictAll(true);
    }

    @Override
    public String toString() {
        return "EvictAllBackupOperation{}";
    }
}
