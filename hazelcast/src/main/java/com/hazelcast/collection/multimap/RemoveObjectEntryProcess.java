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

import com.hazelcast.collection.processor.BackupAwareEntryProcessor;
import com.hazelcast.collection.processor.BaseEntryProcessor;
import com.hazelcast.collection.processor.Entry;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 1/3/13
 */
public class RemoveObjectEntryProcess extends BaseEntryProcessor<Boolean> implements BackupAwareEntryProcessor {

    Data data;

    public RemoveObjectEntryProcess() {
    }

    public RemoveObjectEntryProcess(Data data, boolean binary) {
        super(binary);
        this.data = data;
    }

    public Boolean execute(Entry entry) {
        Collection coll = entry.getValue();
        if (coll == null){
            return false;
        }
        boolean result = false; //coll.remove(isBinary() ? data : IOUtil.toObject(data));
        if (coll.isEmpty()){
            entry.removeEntry();
        }
        return result;
    }

    public void executeBackup(Entry entry) {
        Collection coll = entry.getValue();
        if (coll == null){
            return;
        }
//        coll.remove(isBinary() ? data : IOUtil.toObject(data));
        if (coll.isEmpty()){
            entry.removeEntry();
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeNullableData(out, data);
    }

    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        data = IOUtil.readNullableData(in);
    }
}
