/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

class SlowOperation extends Operation {

    Object response;
    long durationMs;

    @SuppressWarnings("unused")
    SlowOperation() {
    }

    SlowOperation(long durationMs) {
        this.durationMs = durationMs;
    }

    SlowOperation(long durationMs, Object response) {
        this.durationMs = durationMs;
        this.response = response;
    }

    @Override
    public void run() throws Exception {
        Thread.sleep(durationMs);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(durationMs);
        out.writeObject(response);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        durationMs = in.readLong();
        response = in.readObject();
    }
}
