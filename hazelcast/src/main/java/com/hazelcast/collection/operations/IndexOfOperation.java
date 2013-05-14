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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.List;

/**
 * @ali 1/17/13
 */
public class IndexOfOperation extends CollectionKeyBasedOperation {

    Data value;

    boolean last;

    public IndexOfOperation() {
    }

    public IndexOfOperation(CollectionProxyId proxyId, Data dataKey, Data value, boolean last) {
        super(proxyId, dataKey);
        this.value = value;
        this.last = last;
    }

    public void run() throws Exception {
        CollectionWrapper wrapper = getCollectionWrapper();
        if (wrapper != null) {
            List<CollectionRecord> list = (List<CollectionRecord>) wrapper.getCollection();
            CollectionRecord record = new CollectionRecord(isBinary() ? value : toObject(value));
            response = last ? list.lastIndexOf(record) : list.indexOf(record);
        } else {
            response = -1;
        }
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(last);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        last = in.readBoolean();
        value = IOUtil.readData(in);
    }

    public int getId() {
        return CollectionDataSerializerHook.INDEX_OF;
    }
}
