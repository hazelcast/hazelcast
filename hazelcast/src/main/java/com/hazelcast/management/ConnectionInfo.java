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
public class ConnectionInfo implements DataSerializable {

    private int memberIndex;
    private long lastRead;
    private long lastWrite;
    private boolean live;

    public ConnectionInfo() {
    }

    public ConnectionInfo(final int memberIndex, final boolean live, final long lastRead, final long lastWrite) {
        this.lastRead = lastRead;
        this.lastWrite = lastWrite;
        this.live = live;
        this.memberIndex = memberIndex;
    }

    public long getLastRead() {
        return lastRead;
    }

    public long getLastWrite() {
        return lastWrite;
    }

    public boolean isLive() {
        return live;
    }

    public int getMemberIndex() {
        return memberIndex;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(memberIndex);
        out.writeLong(lastRead);
        out.writeLong(lastWrite);
        out.writeBoolean(live);
    }

    public void readData(ObjectDataInput in) throws IOException {
        memberIndex = in.readInt();
        lastRead = in.readLong();
        lastWrite = in.readLong();
        live = in.readBoolean();
    }
}
