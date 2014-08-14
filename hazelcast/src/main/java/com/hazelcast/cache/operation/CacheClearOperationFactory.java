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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CacheDataSerializerHook;
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
 * @author mdogan 06/02/14
 */
public class CacheClearOperationFactory implements OperationFactory, IdentifiedDataSerializable {

    private String name;
    private Set<Data> keys = null;
    private boolean isRemoveAll = false;

    public CacheClearOperationFactory() {
    }

    public CacheClearOperationFactory(String name, Set<Data> keys, boolean isRemoveAll) {
        this.name = name;
        this.keys = keys;
        this.isRemoveAll = isRemoveAll;
    }

    @Override
    public Operation createOperation() {
        return new CacheClearOperation(name, keys, isRemoveAll);
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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(isRemoveAll);
        out.writeBoolean(keys != null);
        if (keys != null) {
            out.write(keys.size());
            for (Data key : keys) {
                key.writeData(out);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        isRemoveAll = in.readBoolean();
        boolean isKeysNotNull = in.readBoolean();
        if (isKeysNotNull) {
            int size = in.readInt();
            keys = new HashSet<Data>(size);
            for (int i = 0; i < size; i++) {
                Data key = new Data();
                key.readData(in);
                keys.add(key);
            }
        }
    }
}
