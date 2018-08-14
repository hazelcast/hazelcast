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
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Operation sent from client to coordinator member to terminate particular
 * job. See also {@link TerminateExecutionOperation}, which is sent from
 * coordinator to members to terminate execution.
 */
public class TerminateJobOperation extends AbstractJobOperation implements IdentifiedDataSerializable {

    private TerminationMode terminationMode;

    public TerminateJobOperation() {
    }

    public TerminateJobOperation(long jobId, TerminationMode mode) {
        super(jobId);
        this.terminationMode = mode;
    }

    @Override
    public void run() {
        JetService service = getService();
        service.getJobCoordinationService().terminateJob(jobId(), terminationMode);
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.TERMINATE_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByte(terminationMode.ordinal());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        terminationMode = TerminationMode.values()[in.readByte()];
    }
}
