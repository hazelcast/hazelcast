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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionRecord;
import com.hazelcast.collection.CollectionWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.List;

/**
 * @ali 1/16/13
 */
public class GetOperation extends CollectionKeyBasedOperation {

    int index;

    public GetOperation() {
    }

    public GetOperation(CollectionProxyId proxyId, Data dataKey, int index) {
        super(proxyId, dataKey);
        this.index = index;
    }

    public void run() throws Exception {
        CollectionWrapper wrapper = getCollectionWrapper();
        List<CollectionRecord> list = (List<CollectionRecord>)wrapper.getCollection();
        try {
            CollectionRecord record = list.get(index);
            response = isBinary() ? (Data) record.getObject() : toData(record.getObject());
        } catch (IndexOutOfBoundsException e) {
            response = e;
        }

    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }

    public int getId() {
        return CollectionDataSerializerHook.GET;
    }
}
