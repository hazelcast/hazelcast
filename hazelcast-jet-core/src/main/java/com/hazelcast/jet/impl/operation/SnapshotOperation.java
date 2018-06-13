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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotOperation extends AsyncOperation {

    private long executionId;
    private long snapshotId;

    // for deserialization
    public SnapshotOperation() {
    }

    public SnapshotOperation(long jobId, long executionId, long snapshotId) {
        super(jobId);
        this.executionId = executionId;
        this.snapshotId = snapshotId;
    }

    @Override
    protected void doRun() {
        JetService service = getService();
        ExecutionContext ctx = service.getJobExecutionService().assertExecutionContext(
                getCallerAddress(), jobId(), executionId, this
        );
        ctx.beginSnapshot(snapshotId).thenAccept(result -> {
            if (result.getError() == null) {
                logFine(getLogger(),
                        "Snapshot %s for job %s finished successfully on member",
                        snapshotId, idToString(jobId()));
            } else {
                getLogger().warning(String.format("Snapshot %d for job %s finished with an error on member",
                        snapshotId, idToString(jobId())), result.getError());
                // wrap the exception
                result.error = new JetException("Exception during snapshot: " + result.error, result.error);
            }
            doSendResponse(result);
        });

    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.SNAPSHOT_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeLong(snapshotId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        snapshotId = in.readLong();
    }

    /**
     * The result of SnapshotOperation with snapshot statistics and error.
     */
    public static final class SnapshotOperationResult implements IdentifiedDataSerializable {
        private long numBytes;
        private long numKeys;
        private long numChunks;
        private Throwable error;

        public SnapshotOperationResult() {
        }

        public SnapshotOperationResult(long numBytes, long numKeys, long numChunks, Throwable error) {
            this.numBytes = numBytes;
            this.numKeys = numKeys;
            this.numChunks = numChunks;
            this.error = error;
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

        public Throwable getError() {
            return error;
        }

        /**
         * Merge other SnapshotOperationResult into this one. It adds the
         * totals and if the other result has error, it will take it to this,
         * unless there already was one.
         */
        public void merge(SnapshotOperationResult other) {
            numBytes += other.numBytes;
            numKeys += other.numKeys;
            numChunks += other.numChunks;
            if (error == null) {
                error = other.error;
            }
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.SNAPSHOT_OPERATION_RESULT;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(numBytes);
            out.writeLong(numKeys);
            out.writeLong(numChunks);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            numBytes = in.readLong();
            numKeys = in.readLong();
            numChunks = in.readLong();
        }
    }
}
