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

package com.hazelcast.multimap.processor;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

/**
 * @ali 1/1/13
 */
public class PutEntryProcessor implements BackupAwareEntryProcessor<Boolean> {

    Data data;

    public PutEntryProcessor() {
    }

    public PutEntryProcessor(Data data) {
        this.data = data;
    }

    public Boolean execute(Entry entry) {
        HashSet<Data> set = entry.getValue();
        System.out.println("added");
        return set.add(data);
    }

    public void executeBackup(Entry entry) {
        HashSet<Data> set = entry.getValue();
        System.out.println("added backup");
        set.add(data);
    }

    public Object createNew() {
        return new HashSet<Data>();
    }

    public void writeData(DataOutput out) throws IOException {
        IOUtil.writeNullableData(out, data);
        data.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        data = IOUtil.readNullableData(in);
    }
}
