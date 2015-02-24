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
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Triggers map loading from a map store
 */
public class LoadMapOperation extends AbstractMapOperation {

    private boolean replaceExistingValues;

    public LoadMapOperation() {
    }

    public LoadMapOperation(String name, boolean replaceExistingValues) {
        super(name);
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public void run() throws Exception {

        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapStoreContext mapStoreContext = mapContainer.getMapStoreContext();

        mapStoreContext.triggerInitialKeyLoad();

        for (int partition :  mapServiceContext.getOwnedPartitions()) {
            RecordStore recordStore = mapServiceContext.getRecordStore(partition, name);
            recordStore.loadAllFromStore(replaceExistingValues);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        replaceExistingValues = in.readBoolean();
    }
}
