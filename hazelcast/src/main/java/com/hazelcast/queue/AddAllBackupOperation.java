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

package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.BackupOperation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @ali 12/20/12
 */
public class AddAllBackupOperation extends QueueOperation implements BackupOperation {

    Set<Data> dataSet;

    public AddAllBackupOperation() {
    }

    public AddAllBackupOperation(String name, Set<Data> dataSet) {
        super(name);
        this.dataSet = dataSet;
    }

    public void run() throws Exception {
        getContainer().addAll(dataSet, true);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataSet.size());
        for (Data data: dataSet){
            IOUtil.writeNullableData(out, data);
        }
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataSet = new HashSet<Data>(size);
        for (int i=0; i<size; i++){
            dataSet.add(IOUtil.readNullableData(in));
        }
    }
}
