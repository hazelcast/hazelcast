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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.impl.JobRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.util.Util.toLocalTime;

/**
 * Runtime information about the job. There's one instance for each jobId, used
 * across multiple executions.
 * <p>
 * It should be updated only through MasterContext class, where multiple
 * updates are synchronized.
 */
public class JobExecutionRecord implements IdentifiedDataSerializable {

    public static final int NO_SNAPSHOT = -1;

    private long jobId;

    /**
     * Timestamp to order async updates to the JobRecord. {@link
     * JobRepository#writeJobExecutionRecord}.
     */
    private final AtomicLong timestamp = new AtomicLong();

    private final AtomicInteger quorumSize = new AtomicInteger();
    private volatile boolean suspended;

    /**
     * The ID of current successful snapshot. If {@link #dataMapIndex} < 0,
     * there's no successful snapshot.
     */
    private volatile long snapshotId = NO_SNAPSHOT;

    /**
     * The data map index of current successful snapshot (0 or 1) or -1, if
     * there's no successful snapshot.
     */
    private volatile int dataMapIndex = -1;

    /*
     * Stats for current successful snapshot.
     */
    private volatile long startTime = Long.MIN_VALUE;
    private volatile long endTime = Long.MIN_VALUE;
    private volatile long numBytes;
    private volatile long numKeys;
    private volatile long numChunks;

    /**
     * ID for the ongoing snapshot. The value is incremented when we start a
     * new snapshot.
     * <p>
     * If {@code ongoingSnapshotId == }{@link #snapshotId}, there's no ongoing
     * snapshot. But if {@code ongoingSnapshotId > }{@link #snapshotId} it
     * doesn't mean a snapshot is ongoing: it will happen when a snapshot
     * fails. For next ongoing snapshot we'll increment the value again.
     */
    private volatile long ongoingSnapshotId = NO_SNAPSHOT;
    private volatile long ongoingSnapshotStartTime;

    /**
     * Contains the error message for the failure of the last snapshot.
     * if the last snapshot was successful, it's null.
     */
    @Nullable
    private volatile String lastFailureText;

    public JobExecutionRecord() {
    }

    public JobExecutionRecord(long jobId, int quorumSize, boolean suspended) {
        this.jobId = jobId;
        this.quorumSize.set(quorumSize);
        this.suspended = suspended;
    }

    public long getJobId() {
        return jobId;
    }

    public int getQuorumSize() {
        return this.quorumSize.get();
    }

    /**
     * Updates the quorum size if it's larger than the current value. Ignores, if it's not.
     */
    void setLargerQuorumSize(int newQuorumSize) {
        this.quorumSize.getAndAccumulate(newQuorumSize, Math::max);
    }

    public boolean isSuspended() {
        return this.suspended;
    }

    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "all updates to ongoingSnapshotId are synchronized")
    public void startNewSnapshot() {
        this.ongoingSnapshotId++;
        this.ongoingSnapshotStartTime = Clock.currentTimeMillis();
    }

    public void ongoingSnapshotDone(long numBytes, long numKeys, long numChunks, @Nullable String failureText) {
        this.lastFailureText = failureText;
        if (failureText == null) {
            this.snapshotId = this.ongoingSnapshotId;
            this.numBytes = numBytes;
            this.numKeys = numKeys;
            this.numChunks = numChunks;
            this.startTime = this.ongoingSnapshotStartTime;
            this.endTime = Clock.currentTimeMillis();
            this.dataMapIndex = ongoingDataMapIndex();
        } else {
            // we don't update the other fields because they only pertain to a successful snapshot
        }
    }

    public long snapshotId() {
        return this.snapshotId;
    }

    public int dataMapIndex() {
        return this.dataMapIndex;
    }

    /**
     * Returns the index of the data map into which the new snapshot will be
     * written.
     */
    int ongoingDataMapIndex() {
        assert this.dataMapIndex == 0 // we'll return 1
                || this.dataMapIndex == 1 // we'll return 0
                || this.dataMapIndex == -1 // we'll return 0
                : "dataMapIndex=" + this.dataMapIndex;
        return (this.dataMapIndex + 1) & 1;
    }

    public long startTime() {
        return this.startTime;
    }

    public long endTime() {
        return this.endTime;
    }

    public long duration() {
        return this.endTime - this.startTime;
    }

    /**
     * Net number of bytes in primary copy. Doesn't include IMap overhead and backup copies.
     */
    public long numBytes() {
        return this.numBytes;
    }

    /**
     * Number of snapshot keys (after exploding chunks).
     */
    public long numKeys() {
        return this.numKeys;
    }

    /**
     * Number of chunks the snapshot is stored in. One chunk is one IMap entry,
     * so this is the number of entries in the data map.
     */
    public long numChunks() {
        return this.numChunks;
    }

    public long ongoingSnapshotId() {
        return this.ongoingSnapshotId;
    }

    public long ongoingSnapshotStartTime() {
        return this.ongoingSnapshotStartTime;
    }

    @Nullable
    public String lastFailureText() {
        return this.lastFailureText;
    }

    long getTimestamp() {
        return timestamp.get();
    }

    /**
     * Sets the timestamp to:
     *   <pre>max(Clock.currentTimeMillis(), this.timestamp + 1);</pre>
     * <p>
     * In other words, after this call the timestamp is guaranteed to be
     * incremented by at least 1 and be no smaller than the current wall clock
     * time.
     */
    void updateTimestamp() {
        timestamp.updateAndGet(v -> Math.max(Clock.currentTimeMillis(), v + 1));
    }

    public String successfulSnapshotDataMapName(long jobId) {
        if (snapshotId() < 0) {
            throw new IllegalStateException("No successful snapshot");
        }
        return snapshotDataMapName(jobId, dataMapIndex());
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.JOB_EXECUTION_RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(snapshotId);
        out.writeInt(dataMapIndex);
        out.writeLong(startTime);
        out.writeLong(endTime);
        out.writeLong(numBytes);
        out.writeLong(numKeys);
        out.writeLong(numChunks);
        out.writeLong(ongoingSnapshotId);
        out.writeLong(ongoingSnapshotStartTime);
        out.writeBoolean(lastFailureText != null);
        if (lastFailureText != null) {
            out.writeUTF(lastFailureText);
        }
        out.writeInt(quorumSize.get());
        out.writeBoolean(suspended);
        out.writeLong(timestamp.get());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        snapshotId = in.readLong();
        dataMapIndex = in.readInt();
        startTime = in.readLong();
        endTime = in.readLong();
        numBytes = in.readLong();
        numKeys = in.readLong();
        numChunks = in.readLong();
        ongoingSnapshotId = in.readLong();
        ongoingSnapshotStartTime = in.readLong();
        lastFailureText = in.readBoolean() ? in.readUTF() : null;
        quorumSize.set(in.readInt());
        suspended = in.readBoolean();
        timestamp.set(in.readLong());
    }

    @Override
    public String toString() {
        return "JobExecutionRecord{" +
                "jobId=" + jobId +
                ", timestamp=" + toLocalTime(timestamp.get()) +
                ", quorumSize=" + quorumSize +
                ", suspended=" + suspended +
                ", snapshotId=" + snapshotId +
                ", dataMapIndex=" + dataMapIndex +
                ", startTime=" + toLocalTime(startTime) +
                ", endTime=" + toLocalTime(endTime) +
                ", numBytes=" + numBytes +
                ", numKeys=" + numKeys +
                ", numChunks=" + numChunks +
                ", ongoingSnapshotId=" + ongoingSnapshotId +
                ", ongoingSnapshotStartTime=" + toLocalTime(ongoingSnapshotStartTime) +
                ", lastFailureText=" + (lastFailureText == null ? "null" : '\'' + lastFailureText + '\'') +
                '}';
    }
}
