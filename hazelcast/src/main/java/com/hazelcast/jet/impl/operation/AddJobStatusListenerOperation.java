/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class AddJobStatusListenerOperation extends AsyncJobOperation implements Versioned {
    private boolean isLightJob;
    private Registration registration;

    public AddJobStatusListenerOperation() {
    }

    public AddJobStatusListenerOperation(long jobId, boolean isLightJob, Registration registration) {
        super(jobId);
        this.isLightJob = isLightJob;
        this.registration = registration;
    }

    @Override
    public CompletableFuture<UUID> doRun() {
        return getJobCoordinationService().addJobStatusListener(jobId(), isLightJob, registration);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.ADD_JOB_STATUS_LISTENER_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(isLightJob);
        registration.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        isLightJob = in.readBoolean();
        registration = new Registration();
        registration.readData(in);
    }
}
