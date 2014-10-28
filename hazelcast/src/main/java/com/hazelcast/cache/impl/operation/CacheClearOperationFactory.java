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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link OperationFactory} implementation for Clear Operations.
 * <p>Clear operation has two main purposes;
 * <ul>
 *     <li>Remove all internal data using <code>isRemoveAll</code>.</li>
 *     <li>Remove the entries of the provided keys.</li>
 * </ul></p>
 * @see OperationFactory
 */
public class CacheClearOperationFactory
        implements OperationFactory, IdentifiedDataSerializable {

    private String name;
    private Set<Data> keys;
    private boolean isClear;
    private int completionId;

    public CacheClearOperationFactory() {
    }

    public CacheClearOperationFactory(String name, Set<Data> keys, boolean isClear, int completionId) {
        this.name = name;
        this.keys = keys;
        this.isClear = isClear;
        this.completionId = completionId;
    }

    @Override
    public Operation createOperation() {
        return new CacheClearOperation(name, keys, isClear, completionId);
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.CLEAR_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeBoolean(isClear);
        out.writeInt(completionId);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.writeInt(keys.size());
            for (Data key : keys) {
                out.writeData(key);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        isClear = in.readBoolean();
        completionId = in.readInt();
        boolean isKeysNotNull = in.readBoolean();
        if (isKeysNotNull) {
            int size = in.readInt();
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                keys.add(key);
            }
        }
    }
}
