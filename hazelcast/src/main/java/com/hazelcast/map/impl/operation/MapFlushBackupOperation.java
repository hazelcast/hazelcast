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

import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

/**
 * Empties backup write-behind-queues upon {@link IMap#flush()}
 */
public class MapFlushBackupOperation extends MapOperation implements BackupOperation, MutatingOperation {

    public MapFlushBackupOperation() {
    }

    public MapFlushBackupOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);
        recordStore.getMapDataStore().clear();
    }
}
