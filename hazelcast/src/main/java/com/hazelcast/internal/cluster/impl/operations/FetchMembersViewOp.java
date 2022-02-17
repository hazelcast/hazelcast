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

package com.hazelcast.internal.cluster.impl.operations;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.MembersView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.exception.CallerNotMemberException;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

/**
 * An operation sent by the member that starts mastership claim process to fetch/gather member views of other members.
 * Collected member views will be used to decide final, most recent member list.
 *
 * @since 3.9
 */
public class FetchMembersViewOp extends AbstractClusterOperation implements JoinOperation {

    private UUID targetUuid;
    private MembersView membersView;

    public FetchMembersViewOp() {
    }

    public FetchMembersViewOp(UUID targetUuid) {
        this.targetUuid = targetUuid;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        UUID thisUuid = service.getLocalMember().getUuid();
        if (!targetUuid.equals(thisUuid)) {
            throw new IllegalStateException("Rejecting mastership claim, since target UUID[" + targetUuid
                    + "] is not matching local member UUID[" + thisUuid + "].");
        }
        membersView = service.handleMastershipClaim(getCallerAddress(), getCallerUuid());
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return membersView;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof CallerNotMemberException) {
            return THROW_EXCEPTION;
        }

        return super.onInvocationException(throwable);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, targetUuid);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        targetUuid = UUIDSerializationUtil.readUUID(in);
    }
}
