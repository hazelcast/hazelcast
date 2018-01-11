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
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Removes keys that are contained in the {@link com.hazelcast.map.impl.recordstore.RecordStore}
 * from the provided list, leaving only keys which are not contained in the record store.
 * <p>
 * This operation is expected to be invoked locally as the key collection is mutated
 * when running the operation and the operation does not have a response object.
 *
 * @see com.hazelcast.core.IMap#loadAll(java.util.Set, boolean)
 */
public class RemoveFromLoadAllOperation extends MapOperation implements PartitionAwareOperation, MutatingOperation {

    private List<Data> keys;

    public RemoveFromLoadAllOperation() {
        keys = Collections.emptyList();
    }

    public RemoveFromLoadAllOperation(String name, List<Data> keys) {
        super(name);
        this.keys = keys;
    }

    @Override
    public void run() throws Exception {
        removeExistingKeys(keys);
    }

    /**
     * Removes keys from the provided collection which
     * are contained in the partition record store.
     *
     * @param keys the keys to be filtered
     */
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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            out.writeData(key);
        }
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
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.REMOVE_FROM_LOAD_ALL;
    }
}
