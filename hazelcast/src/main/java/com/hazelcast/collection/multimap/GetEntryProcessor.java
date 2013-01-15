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

package com.hazelcast.collection.multimap;

import com.hazelcast.collection.processor.Entry;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.List;

/**
 * @ali 1/14/13
 */
public class GetEntryProcessor extends MultiMapEntryProcessor<Data> {

    int index;

    public GetEntryProcessor() {
    }

    public GetEntryProcessor(MultiMapConfig config, int index) {
        super(config.isBinary());
        this.index = index;
    }

    public Data execute(Entry entry) {
        List list = entry.getValue();
        try {
            if (list != null) {
                Object obj = list.get(index);
                return isBinary() ? (Data) obj : entry.getSerializationService().toData(obj);
            }
        } catch (IndexOutOfBoundsException e) {
        }
        return null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(index);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        index = in.readInt();
    }
}
