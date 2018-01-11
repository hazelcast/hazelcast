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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

/**
 * Notifies record store about completion of key loading. The operation should be invoked
 * on a partition thread.
 */
public class KeyLoadStatusOperation extends MapOperation implements PartitionAwareOperation, MutatingOperation {

    private Throwable exception;

    public KeyLoadStatusOperation() {
    }

    public KeyLoadStatusOperation(String name, Throwable exception) {
        super(name);
        this.exception = exception;
    }

    @Override
    public void run() throws Exception {
        recordStore.updateLoadStatus(true, exception);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(exception);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        exception = in.readObject();
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.KEY_LOAD_STATUS;
    }
}
