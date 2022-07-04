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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * Factory implementation for {@link com.hazelcast.cache.impl.operation.CacheGetAllOperation}.
 * @see OperationFactory
 */
public class CacheGetAllOperationFactory
        implements OperationFactory, IdentifiedDataSerializable {

    private String name;
    private Set<Data> keys;
    private ExpiryPolicy expiryPolicy;

    public CacheGetAllOperationFactory() {
        keys = new HashSet<Data>();
    }

    public CacheGetAllOperationFactory(String name, Set<Data> keys, ExpiryPolicy expiryPolicy) {
        this.name = name;
        this.keys = keys;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public Operation createOperation() {
        return new CacheGetAllOperation(name, keys, expiryPolicy);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.GET_ALL_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(name);
        out.writeObject(expiryPolicy);
        out.writeInt(keys.size());
        for (Data key : keys) {
            IOUtil.writeData(out, key);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readString();
        expiryPolicy = in.readObject();
        int size = in.readInt();
        keys = createHashSet(size);
        for (int i = 0; i < size; i++) {
            Data data = IOUtil.readData(in);
            keys.add(data);
        }
    }
}
