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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class CacheKeyIteratorOperation extends AbstractCacheOperation implements ReadonlyOperation {

    private int segmentIndex;
    private int tableIndex;
    private int size;

    public CacheKeyIteratorOperation() {
    }

    public CacheKeyIteratorOperation(String name, int segmentIndex, int tableIndex, int size) {
        super(name, new Data());
        this.segmentIndex = segmentIndex;
        this.tableIndex = tableIndex;
        this.size = size;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.KEY_ITERATOR;
    }

    @Override
    public void run() throws Exception {
        response = cache != null ? this.cache.iterator(segmentIndex, tableIndex, size) : null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(segmentIndex);
        out.writeInt(tableIndex);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        segmentIndex = in.readInt();
        tableIndex = in.readInt();
    }

}
