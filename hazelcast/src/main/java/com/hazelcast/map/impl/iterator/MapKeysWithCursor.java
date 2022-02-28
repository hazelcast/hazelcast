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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;
import java.util.List;

/**
 * Container class for a collection of keys along with pointers defining
 * the iteration state from which new keys can be fetched.
 * This class is usually used when iterating map keys.
 *
 * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator
 */
public class MapKeysWithCursor extends AbstractCursor<Data> {

    public MapKeysWithCursor() {
    }

    public MapKeysWithCursor(List<Data> keys, IterationPointer[] pointers) {
        super(keys, pointers);
    }

    public int getCount() {
        return getBatch() != null ? getBatch().size() : 0;
    }

    @Override
    void writeElement(ObjectDataOutput out, Data element) throws IOException {
        IOUtil.writeData(out, element);
    }

    @Override
    Data readElement(ObjectDataInput in) throws IOException {
        return IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.KEYS_WITH_CURSOR;
    }
}
