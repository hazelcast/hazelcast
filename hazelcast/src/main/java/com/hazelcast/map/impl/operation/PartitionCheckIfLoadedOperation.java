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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class PartitionCheckIfLoadedOperation extends AbstractMapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private boolean isFinished;
    private boolean doLoad;

    public PartitionCheckIfLoadedOperation() {
    }

    public PartitionCheckIfLoadedOperation(String name) {
        super(name);
    }

    public PartitionCheckIfLoadedOperation(String name, boolean doLoad) {
        super(name);
        this.doLoad = doLoad;
    }

    @Override
    public void run() {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(), name);
        isFinished = recordStore.isLoaded();
        if (doLoad) {
            recordStore.maybeDoInitialLoad();
        }
    }

    @Override
    public Object getResponse() {
        return isFinished;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(doLoad);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        doLoad = in.readBoolean();
    }
}
