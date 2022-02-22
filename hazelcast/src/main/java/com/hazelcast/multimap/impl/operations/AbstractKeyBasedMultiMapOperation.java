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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractKeyBasedMultiMapOperation extends AbstractMultiMapOperation implements PartitionAwareOperation {

    protected Data dataKey;
    protected long threadId;

    protected AbstractKeyBasedMultiMapOperation() {
    }

    protected AbstractKeyBasedMultiMapOperation(String name, Data dataKey) {
        super(name);
        this.dataKey = dataKey;
    }

    protected AbstractKeyBasedMultiMapOperation(String name, Data dataKey, long threadId) {
        super(name);
        this.dataKey = dataKey;
        this.threadId = threadId;
    }

    public final void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(threadId);
        IOUtil.writeData(out, dataKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        threadId = in.readLong();
        dataKey = IOUtil.readData(in);
    }
}
