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

package com.hazelcast.multimap.operations;

import com.hazelcast.multimap.MultiMapContainer;
import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @author ali 1/9/13
 */
public class ContainsEntryOperation extends MultiMapOperation {

    Data key;

    Data value;

    public ContainsEntryOperation() {
    }

    public ContainsEntryOperation(String name, Data key, Data value) {
        super(name);
        this.key = key;
        this.value = value;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        ((MultiMapService) getService()).getLocalMultiMapStatsImpl(name).incrementOtherOperations();
        if (key != null && value != null) {
            response = container.containsEntry(isBinary(), key, value);
        } else if (key != null) {
            response = container.containsKey(key);
        } else {
            response = container.containsValue(isBinary(), value);
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
    }

    public int getId() {
        return MultiMapDataSerializerHook.CONTAINS_ENTRY;
    }
}
