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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;

public class ClearBackupOperation extends MapOperation implements BackupOperation, MutatingOperation, IdentifiedDataSerializable
{

    public ClearBackupOperation() {
        this(null);
    }

    public ClearBackupOperation(String name) {
        super(name);
        createRecordStoreOnDemand = false;
    }

    @Override
    public void run() {
        // clear near-cache also on this backup operation, there is a possibility that no owner partition exists on this
        // node but a near-cache exists.
        clearLocalNearCache();

        if (recordStore != null) {
            recordStore.clear();
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.CLEAR_BACKUP;
    }
}
