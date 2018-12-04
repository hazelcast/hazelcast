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

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

public class ExportSnapshotOperation extends AbstractJobOperation {

    private String name;
    private boolean cancelJob;

    public ExportSnapshotOperation() {
    }

    public ExportSnapshotOperation(long jobId, String name, boolean cancelJob) {
        super(jobId);
        this.name = name;
        this.cancelJob = cancelJob;
    }

    @Override
    public void run() {
        JetService service = getService();
        CompletableFuture<Void> future = service.getJobCoordinationService().exportSnapshot(jobId(), name, cancelJob);
        future.whenComplete(withTryCatch(getLogger(), (r, t) -> sendResponse(r != null ? r : t)));
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.EXPORT_SNAPSHOT_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeBoolean(cancelJob);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        cancelJob = in.readBoolean();
    }
}
