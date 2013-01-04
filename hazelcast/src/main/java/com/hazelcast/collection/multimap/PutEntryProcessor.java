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
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * @ali 1/1/13
 */
public class PutEntryProcessor extends BaseEntryProcessor<Boolean> implements BackupAwareEntryProcessor {

    Data data;

    public PutEntryProcessor() {
    }

    public PutEntryProcessor(Data data, boolean binary) {
        super(binary);
        this.data = data;
    }

    public Boolean execute(Entry entry) {
        Collection coll = entry.getOrCreateValue();
        return coll.add(isBinary() ? data : IOUtil.toObject(data));
    }

    public void executeBackup(Entry entry) {
        Collection coll = entry.getOrCreateValue();
        coll.add(isBinary() ? data : IOUtil.toObject(data));
    }

    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeNullableData(out, data);
    }

    public void readData(DataInput in) throws IOException {
        super.readData(in);
        data = IOUtil.readNullableData(in);
    }
}
