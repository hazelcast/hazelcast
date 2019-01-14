/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Objects;
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
    private volatile long snapshotId = NO_SNAPSHOT;
    private volatile int dataMapIndex = -1;
    private volatile long ongoingSnapshotId = NO_SNAPSHOT;
    private volatile long ongoingSnapshotStartTime = Long.MIN_VALUE;
    private volatile String exportedSnapshotMapName;
    @Nullable
    private volatile String lastSnapshotFailure;
    @Nullable
    private volatile SnapshotStats snapshotStats;

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
        return quorumSize.get();
    }

    /**
     * Updates the quorum size if it's larger than the current value. Ignores, if it's not.
     */
    void setLargerQuorumSize(int newQuorumSize) {
        quorumSize.getAndAccumulate(newQuorumSize, Math::max);
    }

    /**
     * Indicates whether job is in suspended state.
     */
    public boolean isSuspended() {
        return suspended;
    }

    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "all updates to ongoingSnapshotId are synchronized")
    public void startNewSnapshot(String exportedSnapshotMapName) {
        ongoingSnapshotId++;
        ongoingSnapshotStartTime = Clock.currentTimeMillis();
        this.exportedSnapshotMapName = exportedSnapshotMapName;
    }

    public SnapshotStats ongoingSnapshotDone(long numBytes, long numKeys, long numChunks, @Nullable String failureText) {
        lastSnapshotFailure = failureText;
        exportedSnapshotMapName = null;
        SnapshotStats res = new SnapshotStats(
                ongoingSnapshotId, ongoingSnapshotStartTime, Clock.currentTimeMillis(), numBytes, numKeys, numChunks
        );
        // switch dataMapIndex only if the snapshot was successful and it wasn't an exported one
        if (failureText == null && exportedSnapshotMapName == null) {
            dataMapIndex = ongoingDataMapIndex();
            snapshotId = ongoingSnapshotId;
            snapshotStats = res;
        }
        ongoingSnapshotStartTime = Long.MIN_VALUE;
        return res;
    }

    /**
     * The ID of current successful snapshot. If {@link #NO_SNAPSHOT} then
     * no successful snapshot exists.
     */
    public long snapshotId() {
        return snapshotId;
    }

    /**
     * The data map index of current successful snapshot (0 or 1) or -1, if
     * there's no successful snapshot.
     */
    public int dataMapIndex() {
        return dataMapIndex;
    }

    /**
     * Returns the index of the data map into which the new snapshot will be
     * written.
     */
    int ongoingDataMapIndex() {
        assert dataMapIndex == 0 // we'll return 1
                || dataMapIndex == 1 // we'll return 0
                || dataMapIndex == -1 // we'll return 0
                : "dataMapIndex=" + dataMapIndex;
        return (dataMapIndex + 1) & 1;
    }

    /**
     * ID for the ongoing or the next snapshot. The value is incremented each
     * time we attempt a new snapshot.
     */
    public long ongoingSnapshotId() {
        return ongoingSnapshotId;
    }

    /**
     * Start time of the ongoing snapshot or {@code Long.MIN_VALUE}, if there's
     * no ongoing snapshot.
     */
    public long ongoingSnapshotStartTime() {
        return ongoingSnapshotStartTime;
    }

    /**
     * Name of the export map. The value is not-null while the job is exporting
     * a state snapshot. The value is null when writing a normal snapshot or
     * when no snapshot is in progress.
     */
    public String exportedSnapshotMapName() {
        return exportedSnapshotMapName;
    }

    /**
     * Stats for the last successful snapshot (except for the exported ones).
     * {@code null} if no successful snapshot exists.
     */
    @Nullable
    public SnapshotStats snapshotStats() {
        return snapshotStats;
    }

    /**
     * Returns the failure message for last snapshot, if any. If last snapshot
     * was successful, then it will return {@code null}.
     */
    @Nullable
    public String lastSnapshotFailure() {
        return lastSnapshotFailure;
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

    String successfulSnapshotDataMapName(long jobId) {
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
        out.writeInt(dataMapIndex);
        out.writeLong(snapshotId);
        out.writeLong(ongoingSnapshotId);
        out.writeInt(quorumSize.get());
        out.writeLong(ongoingSnapshotStartTime);
        // use writeObject instead of writeUTF to allow for nulls
        out.writeObject(lastSnapshotFailure);
        out.writeObject(snapshotStats);
        out.writeObject(exportedSnapshotMapName);
        out.writeBoolean(suspended);
        out.writeLong(timestamp.get());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        dataMapIndex = in.readInt();
        snapshotId = in.readLong();
        ongoingSnapshotId = in.readLong();
        quorumSize.set(in.readInt());
        ongoingSnapshotStartTime = in.readLong();
        lastSnapshotFailure = in.readObject();
        snapshotStats = in.readObject();
        exportedSnapshotMapName = in.readObject();
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
                ", dataMapIndex=" + dataMapIndex +
                ", snapshotId=" + snapshotId +
                ", ongoingSnapshotId=" + ongoingSnapshotId +
                ", ongoingSnapshotStartTime=" + toLocalTime(ongoingSnapshotStartTime) +
                ", snapshotStats=" + snapshotStats +
                ", lastSnapshotFailure=" + (lastSnapshotFailure == null ? "null" : '\'' + lastSnapshotFailure + '\'') +
                '}';
    }

    public static class SnapshotStats implements IdentifiedDataSerializable {

        private long snapshotId;

        /*
         * Stats for current successful snapshot.
         */
        private long startTime;
        private long endTime;
        private long numBytes;
        private long numKeys;
        private long numChunks;

        public SnapshotStats() {
        }

        SnapshotStats(long snapshotId, long startTime, long endTime, long numBytes,
                      long numKeys, long numChunks) {
            this.snapshotId = snapshotId;
            this.startTime = startTime;
            this.endTime = endTime;
            this.numBytes = numBytes;
            this.numKeys = numKeys;
            this.numChunks = numChunks;
        }

        public long startTime() {
            return startTime;
        }

        public long endTime() {
            return endTime;
        }

        public long duration() {
            return endTime - startTime;
        }

        /**
         * Net number of bytes in primary copy. Doesn't include IMap overhead and backup copies.
         */
        public long numBytes() {
            return numBytes;
        }

        /**
         * Number of snapshot keys (after exploding chunks).
         */
        public long numKeys() {
            return numKeys;
        }

        /**
         * Number of chunks the snapshot is stored in. One chunk is one IMap entry,
         * so this is the number of entries in the data map.
         */
        public long numChunks() {
            return numChunks;
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.SNAPSHOT_STATS;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(snapshotId);
            out.writeLong(startTime);
            out.writeLong(endTime);
            out.writeLong(numBytes);
            out.writeLong(numKeys);
            out.writeLong(numChunks);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            snapshotId = in.readLong();
            startTime = in.readLong();
            endTime = in.readLong();
            numBytes = in.readLong();
            numKeys = in.readLong();
            numChunks = in.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotStats that = (SnapshotStats) o;
            return snapshotId == that.snapshotId &&
                    startTime == that.startTime &&
                    endTime == that.endTime &&
                    numBytes == that.numBytes &&
                    numKeys == that.numKeys &&
                    numChunks == that.numChunks;
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshotId, startTime, endTime, numBytes, numKeys, numChunks);
        }

        @Override
        public String toString() {
            return "SnapshotStats{" +
                    "snapshotId=" + snapshotId +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", numBytes=" + numBytes +
                    ", numKeys=" + numKeys +
                    ", numChunks=" + numChunks +
                    '}';
        }
    }
}
