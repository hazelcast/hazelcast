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
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;

/**
 * An operation sent from members to light job coordinator with a list of
 * executionIds to check if the coordinator still knows about those
 * executions.
 * <p>
 * It's used to handle cases when the member misses a TerminateOp (e.g. the
 * execution wasn't yet started when TerminateOp was received) or after a
 * coordinator crashed or if a TerminateOp was handled, but the
 * ExecutionContext was recreated due to a data packet arriving after the
 * cancellation etc.
 */
public class CheckLightJobsOperation extends Operation implements IdentifiedDataSerializable {

    private long[] executionIds;

    public CheckLightJobsOperation() {
    }

    public CheckLightJobsOperation(long[] executionIds) {
        this.executionIds = executionIds;
    }

    @Override
    public Object getResponse() {
        return getJetServiceBackend().getJobCoordinationService().findUnknownExecutions(executionIds);
    }

    protected JetServiceBackend getJetServiceBackend() {
        checkJetIsEnabled(getNodeEngine());
        assert getServiceName().equals(JetServiceBackend.SERVICE_NAME) : "Service is not JetServiceBackend";
        return getService();
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.CHECK_LIGHT_JOBS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLongArray(executionIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionIds = in.readLongArray();
    }
}
