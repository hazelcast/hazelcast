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

import com.hazelcast.collection.CollectionProxyType;
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

    public GetOperation(String name, CollectionProxyType proxyType, Data dataKey, int index) {
        super(name, proxyType, dataKey);
        this.index = index;
    }

    public void run() throws Exception {
        List list = getCollection();
        try {
            if (list != null) {
                Object obj = list.get(index);
                response = isBinary() ? (Data) obj : toData(obj);
            }
        } catch (IndexOutOfBoundsException e) {
        }
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }
}
