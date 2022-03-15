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

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

/**
 * Flushes dirty entries upon call of {@link IMap#flush()}
 */
public class MapFlushOperation extends MapOperation implements BackupAwareOperation, MutatingOperation {

    private long sequence;

    public MapFlushOperation() {
    }

    public MapFlushOperation(String name) {
        super(name);
    }

    @Override
    protected void runInternal() {
        sequence = recordStore.softFlush();
    }

    @Override
    public Object getResponse() {
        return sequence;
    }

    @Override
    public boolean shouldBackup() {
        MapStoreConfig mapStoreConfig = mapContainer.getMapConfig().getMapStoreConfig();
        return mapStoreConfig != null
                && mapStoreConfig.isEnabled()
                && mapStoreConfig.getWriteDelaySeconds() > 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        return new MapFlushBackupOperation(name);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.FLUSH;
    }
}
