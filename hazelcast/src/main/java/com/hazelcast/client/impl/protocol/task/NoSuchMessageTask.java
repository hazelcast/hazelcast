/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemGetCPGroupIdsCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemGetCPObjectInfosCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockGetLockOwnershipCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.MCForceCloseCPSessionCodec;
import com.hazelcast.client.impl.protocol.codec.MCGetCPMembersCodec;
import com.hazelcast.client.impl.protocol.codec.MCPromoteToCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCRemoveCPMemberCodec;
import com.hazelcast.client.impl.protocol.codec.MCResetCPSubsystemCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreChangeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreDrainCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;

import java.security.Permission;
import java.util.Set;

import static com.hazelcast.cp.CPSubsystemStubImpl.CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS;

public class NoSuchMessageTask extends AbstractMessageTask<ClientMessage> {

    private static final Set<Integer> MOVED_CP_MESSAGE_TASKS = Set.of(
            CPGroupCreateCPGroupCodec.REQUEST_MESSAGE_TYPE,
            CPGroupDestroyCPObjectCodec.REQUEST_MESSAGE_TYPE,
            CPSessionCreateSessionCodec.REQUEST_MESSAGE_TYPE,
            CPSessionHeartbeatSessionCodec.REQUEST_MESSAGE_TYPE,
            CPSessionCloseSessionCodec.REQUEST_MESSAGE_TYPE,
            CPSessionGenerateThreadIdCodec.REQUEST_MESSAGE_TYPE,
            CPSubsystemAddMembershipListenerCodec.REQUEST_MESSAGE_TYPE,
            CPSubsystemRemoveMembershipListenerCodec.REQUEST_MESSAGE_TYPE,
            CPSubsystemAddGroupAvailabilityListenerCodec.REQUEST_MESSAGE_TYPE,
            CPSubsystemRemoveGroupAvailabilityListenerCodec.REQUEST_MESSAGE_TYPE,
            CPSubsystemGetCPGroupIdsCodec.REQUEST_MESSAGE_TYPE,
            CPSubsystemGetCPObjectInfosCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongAddAndGetCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongGetAndAddCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongGetCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongGetAndSetCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongApplyCodec.REQUEST_MESSAGE_TYPE,
            AtomicLongAlterCodec.REQUEST_MESSAGE_TYPE,
            AtomicRefApplyCodec.REQUEST_MESSAGE_TYPE,
            AtomicRefSetCodec.REQUEST_MESSAGE_TYPE,
            AtomicRefContainsCodec.REQUEST_MESSAGE_TYPE,
            AtomicRefGetCodec.REQUEST_MESSAGE_TYPE,
            AtomicRefCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
            CountDownLatchAwaitCodec.REQUEST_MESSAGE_TYPE,
            CountDownLatchCountDownCodec.REQUEST_MESSAGE_TYPE,
            CountDownLatchGetCountCodec.REQUEST_MESSAGE_TYPE,
            CountDownLatchGetRoundCodec.REQUEST_MESSAGE_TYPE,
            CountDownLatchTrySetCountCodec.REQUEST_MESSAGE_TYPE,
            FencedLockLockCodec.REQUEST_MESSAGE_TYPE,
            FencedLockTryLockCodec.REQUEST_MESSAGE_TYPE,
            FencedLockUnlockCodec.REQUEST_MESSAGE_TYPE,
            FencedLockGetLockOwnershipCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreAcquireCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreAvailablePermitsCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreChangeCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreDrainCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreGetSemaphoreTypeCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreInitCodec.REQUEST_MESSAGE_TYPE,
            SemaphoreReleaseCodec.REQUEST_MESSAGE_TYPE,
            MCGetCPMembersCodec.REQUEST_MESSAGE_TYPE,
            MCPromoteToCPMemberCodec.REQUEST_MESSAGE_TYPE,
            MCRemoveCPMemberCodec.REQUEST_MESSAGE_TYPE,
            MCResetCPSubsystemCodec.REQUEST_MESSAGE_TYPE,
            MCForceCloseCPSessionCodec.REQUEST_MESSAGE_TYPE
    );

    public NoSuchMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected ClientMessage decodeClientMessage(ClientMessage clientMessage) {
        return clientMessage;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return null;
    }

    @Override
    protected void processMessage() {
        String message = createMessage();
        logger.finest(message);
        throw new UnsupportedOperationException(message);
    }

    @Override
    protected boolean requiresAuthentication() {
        return false;
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    private String createMessage() {
        int messageType = parameters.getMessageType();

        // TODO RU_COMPAT_5_4 added for Version 5.4 compatibility, to provide a meaningful error message
        //  to old clients after moving the CP subsystem to EE
        if (MOVED_CP_MESSAGE_TASKS.contains(messageType)) {
            return CP_SUBSYSTEM_IS_NOT_AVAILABLE_IN_OS;
        } else {
            return "Unrecognized client message received with type: 0x" + Integer.toHexString(messageType);
        }
    }
}
