/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log.impl.operations;

import com.hazelcast.log.impl.LogContainer;
import com.hazelcast.log.impl.LogDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class GetOperation extends LogOperation {
    private long sequence;
    private Object result;

    public GetOperation() {
    }

    public GetOperation(String name, long sequence) {
        super(name);
        this.sequence = sequence;
    }

    @Override
    public void run() throws Exception {
        LogContainer container = getContainer();
        result = container.get(sequence);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public int getClassId() {
        return LogDataSerializerHook.GET;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(sequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.sequence = in.readLong();
    }
}
