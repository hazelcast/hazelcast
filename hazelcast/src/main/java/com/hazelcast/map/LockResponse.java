/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LockResponse implements DataSerializable {
    boolean success = false;
    long version = 0;
    int backupCount = 0;

    public LockResponse() {
    }

    public LockResponse(boolean success, long version, int backupCount) {
        this.success = success;
        this.version = version;
        this.backupCount = backupCount;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeBoolean(success);
        out.writeLong(version);
        out.writeInt(backupCount);
    }

    public void readData(DataInput in) throws IOException {
        success = in.readBoolean();
        version = in.readLong();
        backupCount = in.readInt();
    }

    public boolean getSuccess() {
        return success;
    }

    public long getVersion() {
        return version;
    }

    public int getBackupCount() {
        return backupCount;
    }
}

