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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapWrapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.PartitionAwareOperation;
import java.io.IOException;
import java.util.Collection;

public abstract class MultiMapKeyBasedOperation extends MultiMapOperation implements PartitionAwareOperation {

    protected Data dataKey;
    protected long threadId;

    protected MultiMapKeyBasedOperation() {
    }

    protected MultiMapKeyBasedOperation(String name, Data dataKey) {
        super(name);
        this.dataKey = dataKey;
    }

    protected MultiMapKeyBasedOperation(String name, Data dataKey, long threadId) {
        super(name);
        this.dataKey = dataKey;
        this.threadId = threadId;
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public final MultiMapWrapper getOrCreateCollectionWrapper() {
        return getOrCreateContainer().getOrCreateMultiMapWrapper(dataKey);
    }

    public final MultiMapWrapper getCollectionWrapper() {
        return getOrCreateContainer().getMultiMapWrapper(dataKey);
    }

    public final Collection<MultiMapRecord> remove(boolean copyOf) {
        return getOrCreateContainer().remove(dataKey, copyOf);
    }

    public final void delete() {
        getOrCreateContainer().delete(dataKey);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        dataKey.writeData(out);
        out.writeLong(threadId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataKey = new Data();
        dataKey.readData(in);
        threadId = in.readLong();
    }

}
