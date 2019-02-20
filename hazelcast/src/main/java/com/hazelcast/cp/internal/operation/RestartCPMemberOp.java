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

package com.hazelcast.cp.internal.operation;

import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.Operation;

import java.io.IOException;

import static com.hazelcast.spi.ExecutionService.SYSTEM_EXECUTOR;

/**
 * Resets CP state of a member and restarts the CP subsystem initialization process.
 */
public class RestartCPMemberOp extends Operation implements RaftSystemOperation, IdentifiedDataSerializable {

    private long seed;

    public RestartCPMemberOp() {
    }

    public RestartCPMemberOp(long seed) {
        this.seed = seed;
    }

    @Override
    public CallStatus call() throws Exception {
        return new OffloadImpl();
    }

    private final class OffloadImpl extends Offload {
        private OffloadImpl() {
            super(RestartCPMemberOp.this);
        }

        @Override
        public void start() {
            getNodeEngine().getExecutionService().execute(SYSTEM_EXECUTOR, new RestartLocalTask());
        }
    }

    private class RestartLocalTask implements Runnable {
        @Override
        public void run() {
            RaftService service = getService();
            try {
                service.restartLocal(seed);
                sendResponse(null);
            } catch (Exception e) {
                sendResponse(e);
            }
        }
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public final String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftServiceDataSerializerHook.RESTART_CP_MEMBER_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(seed);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        seed = in.readLong();
    }
}
