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

package com.hazelcast.multimap.impl.operations.client;

import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PortableEntrySetResponse implements Portable {

    Set<Map.Entry> entrySet;

    public PortableEntrySetResponse() {
    }

    public PortableEntrySetResponse(Set<Map.Entry> entrySet) {
        this.entrySet = entrySet;
    }

    public Set<Map.Entry> getEntrySet() {
        return entrySet;
    }

    public int getFactoryId() {
        return MultiMapPortableHook.F_ID;
    }

    public int getClassId() {
        return MultiMapPortableHook.ENTRY_SET_RESPONSE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("s", entrySet.size());
        final ObjectDataOutput out = writer.getRawDataOutput();
        for (Map.Entry<Data, Data> entry : entrySet) {
            Data key = entry.getKey();
            Data value = entry.getValue();
            out.writeData(key);
            out.writeData(value);
        }
    }

    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("s");
        final ObjectDataInput in = reader.getRawDataInput();
        entrySet = new HashSet<Map.Entry>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            entrySet.add(new AbstractMap.SimpleEntry(key, value));
        }
    }
}
