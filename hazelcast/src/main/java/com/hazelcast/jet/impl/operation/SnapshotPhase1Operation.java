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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SnapshotPhase1Operation extends AsyncJobOperation {

    /** If set to true, responses to SnapshotOperation will be postponed until set back to false. */
    // for test
    public static volatile boolean postponeResponses;

    private static final int RETRY_MS = 100;
    private static final CompletableFuture<SnapshotPhase1Result> EMPTY_RESULT =
            completedFuture(new SnapshotPhase1Result(0, 0, 0, null));

    private long executionId;
    private long snapshotId;
    private String mapName;
    private int flags;

    // for deserialization
    public SnapshotPhase1Operation() {
    }

    public SnapshotPhase1Operation(long jobId, long executionId, long snapshotId, String mapName, int flags) {
        super(jobId);
        this.executionId = executionId;
        this.snapshotId = snapshotId;
        this.mapName = mapName;
        this.flags = flags;
    }

    @Override
    protected CompletableFuture<SnapshotPhase1Result> doRun() {
        JetServiceBackend service = getJetServiceBackend();
        ExecutionContext ctx = service.getJobExecutionService().assertExecutionContext(
                getCallerAddress(), jobId(), executionId, getClass().getSimpleName()
        );
        assert !ctx.isLightJob() : "snapshot phase 1 started on a light job: " + idToString(executionId);

        CompletableFuture<SnapshotPhase1Result> future =
            ctx.beginSnapshotPhase1(snapshotId, mapName, flags)
                .exceptionally(exc -> new SnapshotPhase1Result(0, 0, 0, exc))
                .thenApply(result -> {
                    if (result.getError() == null) {
                        logFine(getLogger(),
                                "Snapshot %s phase 1 for %s finished successfully on member",
                                snapshotId, ctx.jobNameAndExecutionId());
                    } else {
                        getLogger().warning(String.format("Snapshot %d phase 1 for %s finished with an error on member: " +
                                        "%s", snapshotId, ctx.jobNameAndExecutionId(), result.getError()));
                    }
                    return result;
                });

        if (!postponeResponses) {
            return future;
        }

        return future.thenCompose(result -> {
            CompletableFuture<SnapshotPhase1Result> f2 = new CompletableFuture<>();
            tryCompleteLater(result, f2);
            return f2;
        });
    }

    private void tryCompleteLater(SnapshotPhase1Result result, CompletableFuture<SnapshotPhase1Result> future) {
        getNodeEngine().getExecutionService().schedule(() -> {
            if (postponeResponses) {
                tryCompleteLater(result, future);
            } else {
                future.complete(result);
            }
        }, RETRY_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SNAPSHOT_PHASE1_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeLong(snapshotId);
        out.writeUTF(mapName);
        out.writeInt(flags);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        snapshotId = in.readLong();
        mapName = in.readUTF();
        flags = in.readInt();
    }

    /**
     * The result of {@link SnapshotPhase1Operation} with snapshot statistics
     * and error.
     */
    public static final class SnapshotPhase1Result implements IdentifiedDataSerializable {
        private long numBytes;
        private long numKeys;
        private long numChunks;
        private String error;

        public SnapshotPhase1Result() {
        }

        public SnapshotPhase1Result(long numBytes, long numKeys, long numChunks, Throwable error) {
            this.numBytes = numBytes;
            this.numKeys = numKeys;
            this.numChunks = numChunks;
            this.error = error == null ? null : requireNonNull(error.toString());
        }

        public long getNumBytes() {
            return numBytes;
        }

        public long getNumKeys() {
            return numKeys;
        }

        public long getNumChunks() {
            return numChunks;
        }

        public String getError() {
            return error;
        }

        /**
         * Merge other SnapshotOperationResult into this one. It adds the
         * subtotals and if the other result has an error, it will store it
         * into this, unless this result already has one.
         */
        public void merge(SnapshotPhase1Result other) {
            numBytes += other.numBytes;
            numKeys += other.numKeys;
            numChunks += other.numChunks;
            if (error == null) {
                error = other.error;
            }
        }

        @Override
        public String toString() {
            return "SnapshotOperationResult{" +
                    "numBytes=" + numBytes +
                    ", numKeys=" + numKeys +
                    ", numChunks=" + numChunks +
                    ", error=" + error +
                    '}';
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.SNAPSHOT_PHASE1_RESULT;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(numBytes);
            out.writeLong(numKeys);
            out.writeLong(numChunks);
            out.writeUTF(error);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            numBytes = in.readLong();
            numKeys = in.readLong();
            numChunks = in.readLong();
            error = in.readUTF();
        }
    }
}
