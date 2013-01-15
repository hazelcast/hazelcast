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

import com.hazelcast.collection.processor.Entry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.List;

/**
 * @ali 1/14/13
 */
public class ContainsEntryProcessor extends MultiMapEntryProcessor<Boolean> {

    Data data;

    public ContainsEntryProcessor() {
    }

    public ContainsEntryProcessor(boolean binary, Data data) {
        super(binary);
        this.data = data;
    }

    public Boolean execute(Entry entry) {
        List list = entry.getValue();
        if (list !=null){
            return list.contains(isBinary() ? data : entry.getSerializationService().toObject(data));
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        data.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        data = new Data();
        data.readData(in);
    }
}
