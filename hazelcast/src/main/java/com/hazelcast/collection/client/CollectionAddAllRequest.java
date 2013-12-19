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

package com.hazelcast.collection.client;

import com.hazelcast.collection.CollectionAddAllOperation;
import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ali 9/4/13
 */
public class CollectionAddAllRequest extends CollectionRequest {

    protected List<Data> valueList;

    public CollectionAddAllRequest() {
    }

    public CollectionAddAllRequest(String name, List<Data> valueList) {
        super(name);
        this.valueList = valueList;
    }

    protected Operation prepareOperation() {
        return new CollectionAddAllOperation(name, valueList);
    }

    public int getClassId() {
        return CollectionPortableHook.COLLECTION_ADD_ALL;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        super.writePortable(writer);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeInt(valueList.size());
        for (Data value : valueList) {
            value.writeData(out);
        }
    }

    public void readPortable(PortableReader reader) throws IOException {
        super.readPortable(reader);
        final ObjectDataInput in = reader.getRawDataInput();
        final int size = in.readInt();
        valueList = new ArrayList<Data>(size);
        for (int i=0; i<size; i++){
            final Data value = new Data();
            value.readData(in);
            valueList.add(value);
        }
    }

    public String getRequiredAction() {
        return ActionConstants.ACTION_ADD;
    }
}
