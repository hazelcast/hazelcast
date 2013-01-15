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

package com.hazelcast.management;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @mdogan 5/8/12
 */
public class LockInfo implements DataSerializable {

    private String name;
    private String key;
    private int ownerMemberIndex;
    private long acquireTime;
    private int waitingThreadCount;

    public LockInfo() {
    }

    public LockInfo(final String name, final String key, final long acquireTime,
             final int ownerMemberIndex, final int waitingThreadCount) {
        this.acquireTime = acquireTime;
        this.key = key;
        this.name = name;
        this.ownerMemberIndex = ownerMemberIndex;
        this.waitingThreadCount = waitingThreadCount;
    }

    public long getAcquireTime() {
        return acquireTime;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public int getOwnerMemberIndex() {
        return ownerMemberIndex;
    }

    public int getWaitingThreadCount() {
        return waitingThreadCount;
    }

    public void readData(final ObjectDataInput in) throws IOException {
        name = in.readUTF();
        key = in.readUTF();
        ownerMemberIndex = in.readInt();
        acquireTime = in.readLong();
        waitingThreadCount = in.readInt();
    }

    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(key);
        out.writeInt(ownerMemberIndex);
        out.writeLong(acquireTime);
        out.writeInt(waitingThreadCount);
    }
}
