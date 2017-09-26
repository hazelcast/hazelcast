/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.idToString;

public class SnapshotOperation extends AsyncExecutionOperation {

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
    protected void doRun() throws Exception {
        JetService service = getService();
        service.getJobExecutionService()
               .beginSnapshot(getCallerAddress(), jobId, executionId, snapshotId)
               .thenAccept(r -> {
                   logFine(getLogger(),
                           "Snapshot %s for job %s finished successfully on member",
                           snapshotId, idToString(jobId));
                   doSendResponse(null);
               })
               .exceptionally(e -> {
                   getLogger().warning(String.format("Snapshot %d for job %s finished with error on member",
                           snapshotId, idToString(jobId)), e);
                   doSendResponse(new JetException("Exception during snapshot", e));
                   return null;
               });

    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.SNAPSHOT_OP;
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
}
