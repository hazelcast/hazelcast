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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class SnapshotPhase2Operation extends AsyncJobOperation {

    private static final CompletableFuture<Void> EMPTY_RESULT = completedFuture(null);

    private long executionId;
    private long snapshotId;
    private boolean success;

    // for deserialization
    public SnapshotPhase2Operation() {
    }

    public SnapshotPhase2Operation(long jobId, long executionId, long snapshotId, boolean success) {
        super(jobId);
        this.executionId = executionId;
        this.snapshotId = snapshotId;
        this.success = success;
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        JetServiceBackend service = getJetServiceBackend();
        ExecutionContext ctx = service.getJobExecutionService().assertExecutionContext(
                getCallerAddress(), jobId(), executionId, getClass().getSimpleName()
        );
        assert !ctx.isLightJob() : "snapshot phase 2 started on a light job: " + idToString(executionId);

        return ctx.beginSnapshotPhase2(snapshotId, success)
                  .whenComplete((r, t) -> {
                    if (t != null) {
                        getLogger().warning(
                                String.format("Snapshot %d phase 2 for %s finished with an error on member: %s",
                                snapshotId, ctx.jobNameAndExecutionId(), t), t);
                    } else {
                        logFine(getLogger(), "Snapshot %s phase 2 for %s finished successfully on member",
                                snapshotId, ctx.jobNameAndExecutionId());
                    }
                });
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SNAPSHOT_PHASE2_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeLong(snapshotId);
        out.writeBoolean(success);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        snapshotId = in.readLong();
        success = in.readBoolean();
    }
}

