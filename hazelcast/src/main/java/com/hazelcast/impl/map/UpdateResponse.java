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

package com.hazelcast.impl.map;

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UpdateResponse implements DataSerializable {
    Data oldValue;
    long version;
    int backupCount;

    public UpdateResponse() {
    }

    public UpdateResponse(Data oldValue, long version, int backupCount) {
        this.oldValue = oldValue;
        this.version = version;
        this.backupCount = backupCount;
    }

    public void writeData(DataOutput out) throws IOException {
        IOUtil.writeNullableData(out, oldValue);
        out.writeLong(version);
        out.writeInt(backupCount);
    }

    public void readData(DataInput in) throws IOException {
        oldValue = IOUtil.readNullableData(in);
        version = in.readLong();
        backupCount = in.readInt();
    }

    public Data getOldValue() {
        return oldValue;
    }

    public long getVersion() {
        return version;
    }

    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public String toString() {
        return "UpdateResponse{" +
                "oldValue=" + oldValue +
                ", version=" + version +
                ", backupCount=" + backupCount +
                '}';
    }
}
