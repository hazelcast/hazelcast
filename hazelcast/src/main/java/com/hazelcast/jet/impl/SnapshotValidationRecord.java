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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;

public class SnapshotValidationRecord implements IdentifiedDataSerializable {
    public static final SnapshotValidationKey KEY = SnapshotValidationKey.KEY;

    private long snapshotId;
    private long numChunks;
    private long numBytes;

    private long creationTime;
    private long jobId;
    private String jobName;
    private String dagJsonString;

    public SnapshotValidationRecord() {
    }

    SnapshotValidationRecord(long snapshotId, long numChunks, long numBytes, long creationTime, long jobId,
                                    @Nonnull String jobName, @Nonnull String dagJsonString) {
        this.snapshotId = snapshotId;
        this.numChunks = numChunks;
        this.numBytes = numBytes;
        this.creationTime = creationTime;
        this.jobId = jobId;
        this.jobName = jobName;
        this.dagJsonString = dagJsonString;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long numChunks() {
        return numChunks;
    }

    public long numBytes() {
        return numBytes;
    }

    public long creationTime() {
        return creationTime;
    }

    public long jobId() {
        return jobId;
    }

    public String jobName() {
        return jobName;
    }

    public String dagJsonString() {
        return dagJsonString;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SNAPSHOT_VALIDATION_RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(snapshotId);
        out.writeLong(numChunks);
        out.writeLong(numBytes);
        out.writeLong(creationTime);
        out.writeLong(jobId);
        out.writeUTF(jobName);
        out.writeUTF(dagJsonString);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        snapshotId = in.readLong();
        numChunks = in.readLong();
        numBytes = in.readLong();
        creationTime = in.readLong();
        jobId = in.readLong();
        jobName = in.readUTF();
        dagJsonString = in.readUTF();
    }

    enum SnapshotValidationKey {
        KEY
    }
}
