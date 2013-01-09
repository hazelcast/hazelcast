/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @ali 1/9/13
 */
public class ContainsOperation extends MultiMapOperation {

    boolean binary;

    Data key;

    Data value;

    public ContainsOperation() {
    }

    public ContainsOperation(String name, boolean binary, Data key, Data value) {
        super(name);
        this.binary = binary;
        this.key = key;
        this.value = value;
    }

    public void run() throws Exception {
        CollectionContainer container = getContainer();
        response = container.contains(binary, key, value);

    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(binary);
        IOUtil.writeNullableData(out, key);
        IOUtil.writeNullableData(out, value);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        binary = in.readBoolean();
        key = IOUtil.readNullableData(in);
        value = IOUtil.readNullableData(in);
    }
}
