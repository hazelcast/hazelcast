/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.countdownlatch.CountDownLatchContainer;
import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.COUNT_DOWN_LATCH_REPLICATION_OPERATION;
import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.F_ID;
import static com.hazelcast.concurrent.countdownlatch.CountDownLatchService.SERVICE_NAME;

public class CountDownLatchReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private Collection<CountDownLatchContainer> data;

    public CountDownLatchReplicationOperation() {
    }

    public CountDownLatchReplicationOperation(Collection<CountDownLatchContainer> data) {
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        if (data == null) {
            return;
        }

        CountDownLatchService service = getService();
        for (CountDownLatchContainer container : data) {
            service.add(container);
        }
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return COUNT_DOWN_LATCH_REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = data != null ? data.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (CountDownLatchContainer container : data) {
                container.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            data = new ArrayList<CountDownLatchContainer>(len);
            for (int i = 0; i < len; i++) {
                CountDownLatchContainer container = new CountDownLatchContainer();
                container.readData(in);
                data.add(container);
            }
        }
    }
}
