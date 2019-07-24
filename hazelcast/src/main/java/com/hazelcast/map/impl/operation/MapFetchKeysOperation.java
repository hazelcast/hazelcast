/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

/**
 * Operation for fetching a chunk of keys from a single {@link IMap} partition.
 * The starting offset is defined by the {@link #lastTableIndex} and the soft limit is defined by the {@link #fetchSize}.
 *
 * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator(int, int, boolean)
 */
public class MapFetchKeysOperation extends MapOperation implements ReadonlyOperation {

    private int fetchSize;
    private int lastTableIndex;
    private transient MapKeysWithCursor response;

    public MapFetchKeysOperation() {
    }

    public MapFetchKeysOperation(String name, int lastTableIndex, int fetchSize) {
        super(name);
        this.lastTableIndex = lastTableIndex;
        this.fetchSize = fetchSize;
    }

    @Override
    protected void runInternal() {
        response = recordStore.fetchKeys(lastTableIndex, fetchSize);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        fetchSize = in.readInt();
        lastTableIndex = in.readInt();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(fetchSize);
        out.writeInt(lastTableIndex);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.FETCH_KEYS;
    }
}
