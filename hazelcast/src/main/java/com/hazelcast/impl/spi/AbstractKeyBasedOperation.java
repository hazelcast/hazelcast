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

package com.hazelcast.impl.spi;

import com.hazelcast.nio.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class AbstractKeyBasedOperation extends Operation {
    protected Data dataKey = null;
    protected int threadId = -1;

    public AbstractKeyBasedOperation(Data dataKey) {
        this.dataKey = dataKey;
    }

    public AbstractKeyBasedOperation() {
    }

    public Data getKey() {
        return dataKey;
    }

    public int getThreadId() {
        return threadId;
    }

    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }

    public void writeInternal(DataOutput out) throws IOException {
        dataKey.writeData(out);
        out.writeInt(threadId);
    }

    public void readInternal(DataInput in) throws IOException {
        dataKey = new Data();
        dataKey.readData(in);
        threadId = in.readInt();
    }
}
