/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Triggers loading values for the given keys from the defined
 * {@link com.hazelcast.core.MapLoader}.
 * The values are loaded asynchronously and the loaded key-value pairs are sent to
 * the partition threads to update the record stores.
 * <p>
 * This operation is executed on the partition thread and loads values for keys
 * provided by the {@link com.hazelcast.map.impl.MapKeyLoader.Role#SENDER}.
 */
public class LoadAllOperation extends MapOperation implements PartitionAwareOperation, MutatingOperation {

    private List<Data> keys;

    private boolean replaceExistingValues;

    public LoadAllOperation() {
        keys = Collections.emptyList();
    }

    public LoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public void run() throws Exception {
        keys = selectThisPartitionsKeys();
        recordStore.loadAllFromStore(keys, replaceExistingValues);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        invalidateNearCache(keys);
    }

    /**
     * Filters the {@link #keys} list for keys matching the partition on
     * which this operation is executed.
     *
     * @return the filtered key list
     */
    private List<Data> selectThisPartitionsKeys() {
        final IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        final int partitionId = getPartitionId();
        List<Data> dataKeys = null;
        for (Data key : keys) {
            if (partitionId == partitionService.getPartitionId(key)) {
                if (dataKeys == null) {
                    dataKeys = new ArrayList<Data>(keys.size());
                }
                dataKeys.add(key);
            }
        }
        if (dataKeys == null) {
            return Collections.emptyList();
        }
        return dataKeys;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            out.writeData(key);
        }
        out.writeBoolean(replaceExistingValues);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<Data>(size);
        }
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
        replaceExistingValues = in.readBoolean();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.LOAD_ALL;
    }
}
