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
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.RaftServiceDataSerializerHook;
import com.hazelcast.cp.internal.RaftSystemOperation;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Returns the CPMemberInfo for the local member if a CP member exists. Otherwise, it returns null.
 */
public class GetLocalCPMemberOp extends Operation implements RaftSystemOperation, IdentifiedDataSerializable {

    private transient CPMemberInfo cpMemberInfo;

    public GetLocalCPMemberOp() {
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        cpMemberInfo = service.getLocalCPMember();
    }

    @Override
    public Object getResponse() {
        return cpMemberInfo;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
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
    public int getClassId() {
        return RaftServiceDataSerializerHook.GET_LOCAL_CP_MEMBER;
    }
}
