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

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @ali 12/19/12
 */
public class DrainOperation extends QueueBackupAwareOperation {

    int maxSize = -1;

    //TODO how about waiting polls

    public DrainOperation() {
    }

    public DrainOperation(String name, int maxSize) {
        super(name);
        this.maxSize = maxSize;
    }

    public void run() throws Exception {
        response = getContainer().drain(maxSize);
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null){
            List<Data> list = (List<Data>)response;
            for (Data data: list){
                publishEvent(ItemEventType.REMOVED, data);
            }
        }
    }

    public boolean shouldBackup() {
        if (response != null){
            List<Data> list = (List<Data>)response;
            return list.size() > 0;
        }
        return false;
    }

    public Operation getBackupOperation() {
        return new DrainBackupOperation(name, maxSize);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(maxSize);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        maxSize = in.readInt();
    }
}
