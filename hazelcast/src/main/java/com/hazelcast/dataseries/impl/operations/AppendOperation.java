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

package com.hazelcast.dataseries.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook.APPEND_OPERATION;

public class AppendOperation extends DataSeriesOperation {

    private byte[] value;
    private transient long sequence;

    public AppendOperation() {
    }

    public AppendOperation(String name, byte[] value) {
        super(name);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        sequence = partition.append(value);
    }

    @Override
    public Object getResponse() {
        return sequence;
    }

    @Override
    public int getId() {
        return APPEND_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByteArray(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readByteArray();
    }
}
