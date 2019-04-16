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

package com.hazelcast.datastream.impl.operations;

import com.hazelcast.datastream.impl.DSDataSerializerHook;
import com.hazelcast.datastream.impl.DSPartitionListeners;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.function.Consumer;

import java.io.IOException;

public class AddListenerOperation extends DataStreamOperation {

    private transient Consumer consumer;
    private long offset;

    public AddListenerOperation() {
    }

    public AddListenerOperation(String name, long offset, Consumer consumer) {
        super(name);
        this.offset = offset;
        this.consumer = consumer;
    }

    @Override
    public void run() throws Exception {
        DSPartitionListeners listeners = service.getOrCreatePartitionListeners(getName(), getPartitionId(), partition);
        if (executedLocally()) {
            listeners.registerLocalListener(consumer, offset);
        } else {
            listeners.registerRemoteListener(getCallerUuid(), getConnection(), offset);
        }
    }

    @Override
    public int getId() {
        return DSDataSerializerHook.ADD_LISTENER_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(offset);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.offset = in.readLong();
    }
}
