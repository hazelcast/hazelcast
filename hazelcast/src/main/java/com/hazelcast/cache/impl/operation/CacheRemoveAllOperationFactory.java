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

import java.io.IOException;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * {@link OperationFactory} implementation for RemoveAll Operations.
 * <p>RemoveAll operation has two main purposes;
 * <ul>
 * <li>Remove all internal data
 * <li>Remove the entries of the provided keys.</li>
 * </ul>
 *
 * @see OperationFactory
 */
public class CacheRemoveAllOperationFactory implements OperationFactory, IdentifiedDataSerializable {

    private String name;

    private Set<Data> keys;

    private int completionId;

    public CacheRemoveAllOperationFactory() {
    }

    public CacheRemoveAllOperationFactory(String name, Set<Data> keys, int completionId) {
        this.name = name;
        this.keys = keys;
        this.completionId = completionId;
    }

    @Override
    public Operation createOperation() {
        return new CacheRemoveAllOperation(name, keys, completionId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(completionId);
        out.writeInt(keys == null ? -1 : keys.size());
        if (keys != null) {
            for (Data key : keys) {
                IOUtil.writeData(out, key);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        completionId = in.readInt();
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        keys = createHashSet(size);
        for (int i = 0; i < size; i++) {
            keys.add(IOUtil.readData(in));
        }
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.REMOVE_ALL_FACTORY;
    }
}
