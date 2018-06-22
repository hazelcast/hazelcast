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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

/**
 * <p>Provides iterator functionality for ICache.</p>
 * <p>
 * Initializes and grabs a number of keys defined by <code>size</code> parameter from the
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} with the last table index.
 * </p>
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#fetchKeys(int, int)
 */
public class CacheKeyIteratorOperation
        extends KeyBasedCacheOperation
        implements ReadonlyOperation {

    private int tableIndex;
    private int size;

    public CacheKeyIteratorOperation() {
    }

    public CacheKeyIteratorOperation(String name, int tableIndex, int size) {
        super(name, new HeapData());
        this.tableIndex = tableIndex;
        this.size = size;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.KEY_ITERATOR;
    }

    @Override
    public void run() throws Exception {
        response = recordStore.fetchKeys(tableIndex, size);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(tableIndex);
        out.writeInt(size);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        tableIndex = in.readInt();
        size = in.readInt();
    }

}
