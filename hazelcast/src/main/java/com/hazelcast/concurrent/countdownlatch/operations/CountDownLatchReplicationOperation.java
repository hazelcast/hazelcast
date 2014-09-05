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

package com.hazelcast.concurrent.countdownlatch.operations;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchInfo;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class CountDownLatchReplicationOperation extends AbstractOperation implements IdentifiedDataSerializable {

    private Collection<CountDownLatchInfo> data;

    public CountDownLatchReplicationOperation() {
    }

    public CountDownLatchReplicationOperation(Collection<CountDownLatchInfo> data) {
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        if (data != null) {
            CountDownLatchService service = getService();
            for (CountDownLatchInfo latchInfo : data) {
                service.add(latchInfo);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = data != null ? data.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (CountDownLatchInfo latch : data) {
                latch.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            data = new ArrayList<CountDownLatchInfo>();
            for (int i = 0; i < len; i++) {
                CountDownLatchInfo latch = new CountDownLatchInfo();
                latch.readData(in);
                data.add(latch);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return CountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_REPLICATION_OPERATION;
    }

}
