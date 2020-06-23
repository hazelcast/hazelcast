/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.PartitionRuntimeState;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.version.Version;

import java.io.IOException;

/**
 * Sent from the master to publish or sync the partition table state to all cluster members.
 *
 * @see InternalPartitionServiceImpl#publishPartitionRuntimeState
 * @see InternalPartitionServiceImpl#syncPartitionRuntimeState
 */
public final class PartitionStateOperation extends AbstractPartitionOperation implements MigrationCycleOperation, Versioned {

    private PartitionRuntimeState partitionState;
    private boolean sync;
    private boolean success;

    public PartitionStateOperation() {
    }

    public PartitionStateOperation(PartitionRuntimeState partitionState, boolean sync) {
        this.partitionState = partitionState;
        this.sync = sync;
    }

    @Override
    public void run() {
        Address callerAddress = getCallerAddress();
        partitionState.setMaster(callerAddress);
        InternalPartitionServiceImpl partitionService = getService();
        success = partitionService.processPartitionRuntimeState(partitionState);

        ILogger logger = getLogger();
        if (logger.isFineEnabled()) {
            String message = (success ? "Applied" : "Rejected")
                    + " new partition state. Version: " + partitionState.getVersion() + ", caller: " + callerAddress;
            logger.fine(message);
        }
    }

    @Override
    public boolean returnsResponse() {
        return sync;
    }

    @Override
    public Object getResponse() {
        return success;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException
                || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        // RU_COMPAT_3_11
        Version version = in.getVersion();
        if (version.isGreaterOrEqual(Versions.V3_12)) {
            partitionState = in.readObject();
        } else {
            partitionState = new PartitionRuntimeState();
            partitionState.readData(in);
        }
        sync = in.readBoolean();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        // RU_COMPAT_3_11
        Version version = out.getVersion();
        if (version.isGreaterOrEqual(Versions.V3_12)) {
            out.writeObject(partitionState);
        } else {
            partitionState.writeData(out);
        }
        out.writeBoolean(sync);
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.PARTITION_STATE_OP;
    }
}
