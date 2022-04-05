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
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Container class for a collection of entries along with pointers defining
 * the iteration state from which new keys can be fetched.
 * This class is usually used when iterating map entries.
 *
 * @see com.hazelcast.map.impl.proxy.MapProxyImpl#iterator
 */
public class MapEntriesWithCursor extends AbstractCursor<Map.Entry<Data, Data>> {

    public MapEntriesWithCursor() {
    }

    public MapEntriesWithCursor(List<Map.Entry<Data, Data>> entries, IterationPointer[] pointers) {
        super(entries, pointers);
    }

    @Override
    void writeElement(ObjectDataOutput out, Entry<Data, Data> entry) throws IOException {
        IOUtil.writeData(out, entry.getKey());
        IOUtil.writeData(out, entry.getValue());
    }

    @Override
    Entry<Data, Data> readElement(ObjectDataInput in) throws IOException {
        final Data key = IOUtil.readData(in);
        final Data value = IOUtil.readData(in);
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.ENTRIES_WITH_CURSOR;
    }
}
