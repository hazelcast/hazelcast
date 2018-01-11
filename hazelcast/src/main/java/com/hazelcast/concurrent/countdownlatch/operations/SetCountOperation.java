/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

import static com.hazelcast.concurrent.countdownlatch.CountDownLatchDataSerializerHook.SET_COUNT_OPERATION;

public class SetCountOperation extends BackupAwareCountDownLatchOperation implements MutatingOperation {

    private int count;
    private boolean response;

    public SetCountOperation() {
    }

    public SetCountOperation(String name, int count) {
        super(name);
        this.count = count;
    }

    @Override
    public void run() throws Exception {
        CountDownLatchService service = getService();
        response = service.setCount(name, count);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public boolean shouldBackup() {
        return response;
    }

    @Override
    public int getId() {
        return SET_COUNT_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(count);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        count = in.readInt();
    }
}
