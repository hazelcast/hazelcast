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

package com.hazelcast.cp.internal.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Triggers the local CP member to transfer Raft group leadership to given CP
 * member for the given CP group
 */
public class TransferLeadershipOp extends Operation implements RaftSystemOperation, IdentifiedDataSerializable,
                                                               BiConsumer<Object, Throwable> {

    private CPGroupId groupId;
    private CPMember destination;

    public TransferLeadershipOp() {
    }

    public TransferLeadershipOp(CPGroupId groupId, CPMember destination) {
        this.groupId = groupId;
        this.destination = destination;
    }

    @Override
    public CallStatus call() throws Exception {
        RaftService service = getService();
        InternalCompletableFuture future = service.transferLeadership(groupId, (CPMemberInfo) destination);
        future.whenCompleteAsync(this);
        return CallStatus.VOID;
    }

    @Override
    public void accept(Object response, Throwable throwable) {
        if (throwable == null) {
            sendResponse(response);
        } else {
            sendResponse(throwable);
        }
    }

    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
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
    public int getClassId() {
        return RaftServiceDataSerializerHook.TRANSFER_LEADERSHIP_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(groupId);
        out.writeObject(destination);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        groupId = in.readObject();
        destination = in.readObject();
    }
}
