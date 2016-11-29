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

import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.partition.IPartitionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Triggers map store load of all given keys.
 */
public class LoadAllOperation extends MapOperation implements PartitionAwareOperation, MutatingOperation {

    private List<Data> keys;

    private boolean replaceExistingValues;
    private boolean withUserSuppliedKeys;

    public LoadAllOperation() {
        keys = Collections.emptyList();
    }

    public LoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues, boolean withUserSuppliedKeys) {
        super(name);
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
        this.withUserSuppliedKeys = withUserSuppliedKeys;
        // the withUserSuppliedKeys field piggy-backs on Operation flags for serialization
        // as the field was introduced in a patch release
        setFlag(withUserSuppliedKeys, BITMASK_LOAD_ALL_WITH_USER_SUPPLIED_KEYS);
    }

    @Override
    public void run() throws Exception {
        keys = selectThisPartitionsKeys();

        if (!replaceExistingValues) {
            removeExistingKeys(keys);
        }

        recordStore.loadAllFromStore(keys, withUserSuppliedKeys);
    }

    @Override
    public void afterRun() throws Exception {
        if (replaceExistingValues) {
            clearLocalNearCache();
        }

        super.afterRun();
    }

    private void removeExistingKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        Storage storage = recordStore.getStorage();
        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            if (storage.containsKey(key)) {
                iterator.remove();
            }
        }
    }

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
        withUserSuppliedKeys = isFlagSet(BITMASK_LOAD_ALL_WITH_USER_SUPPLIED_KEYS);

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
}
