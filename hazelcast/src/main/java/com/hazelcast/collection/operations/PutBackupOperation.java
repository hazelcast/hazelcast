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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionProxyType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @ali 1/16/13
 */
public class PutBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    Data value;

    int index;

    public PutBackupOperation() {
    }

    public PutBackupOperation(String name, CollectionProxyType proxyType, Data dataKey, Data value, int index) {
        super(name, proxyType, dataKey);
        this.value = value;
        this.index = index;
    }

    public void run() throws Exception {
        Object obj = isBinary() ? value : toObject(value);
        if (index == -1) {
            Collection coll = getOrCreateCollection();
            response = coll.add(obj);
        } else {
            List list = getOrCreateCollection();
            list.add(index, obj);
            response = true;
        }
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
        value.writeData(out);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
        value = IOUtil.readData(in);
    }
}
