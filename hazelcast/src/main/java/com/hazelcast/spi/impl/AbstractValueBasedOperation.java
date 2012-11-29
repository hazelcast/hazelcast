/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class AbstractValueBasedOperation extends Operation {
    protected Data dataValue = null;
    protected int threadId = -1;
    protected long timeout = -1;

    public AbstractValueBasedOperation(Data dataValue) {
        this.dataValue = dataValue;
    }

    public AbstractValueBasedOperation() {
    }

    public Data getKey() {
        return dataValue;
    }

    public int getThreadId() {
        return threadId;
    }

    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void writeInternal(DataOutput out) throws IOException {
        dataValue.writeData(out);
        out.writeInt(threadId);
        out.writeLong(timeout);
    }

    public void readInternal(DataInput in) throws IOException {
        dataValue = new Data();
        dataValue.readData(in);
        threadId = in.readInt();
        timeout = in.readLong();
    }
}
