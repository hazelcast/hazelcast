/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;

class OffHeapRecord extends AbstractRecord<Data> {

    private Storage<DataRef> storage;
    private DataRef valueRef;

    OffHeapRecord() {
    }

    OffHeapRecord(Storage<DataRef> storage, Data key, Data value) {
        super(key);
        this.storage = storage;
        setValue(value);
    }

    @Override
    public long getCost() {
        long size = super.getCost();
        final int objectReferenceInBytes = 4;
        // storage ref
        size += objectReferenceInBytes;
        // value size
        size += objectReferenceInBytes + (valueRef == null ? 0 : valueRef.heapCost());
        return size;
    }

    public Data getValue() {
        if (valueRef != null) {
            return storage.get(key.getPartitionHash(), valueRef);
        }
        return null;
    }

    public void setValue(Data value) {
        invalidate();
        if (value != null) {
            valueRef = storage.put(key.getPartitionHash(), value);
        }
    }

    public void invalidate() {
        if (valueRef != null) {
            storage.remove(key.getPartitionHash(), valueRef);
        }
        valueRef = null;
    }
}
