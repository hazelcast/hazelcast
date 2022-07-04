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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class PutWithExpiryOperation extends PutOperation {

    private long ttl;
    private long maxIdle;

    public PutWithExpiryOperation() {
    }

    public PutWithExpiryOperation(String name, Data dataKey,
                                  Data value, long ttl, long maxIdle) {
        super(name, dataKey, value);
        this.ttl = ttl;
        this.maxIdle = maxIdle;
    }

    @Override
    protected long getTtl() {
        return ttl;
    }

    @Override
    protected long getMaxIdle() {
        return maxIdle;
    }

    @Override
    public Object getResponse() {
        return oldValue;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_WITH_EXPIRY;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeLong(ttl);
        out.writeLong(maxIdle);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        ttl = in.readLong();
        maxIdle = in.readLong();
    }
}
