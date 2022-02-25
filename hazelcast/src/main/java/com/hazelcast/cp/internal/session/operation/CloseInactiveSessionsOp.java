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

package com.hazelcast.cp.internal.session.operation;

import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.internal.session.RaftSessionServiceDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Closes inactive sessions. A session is considered to be inactive
 * if it is committing heartbeats but has not acquired any resource for
 * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()}. There is no point
 * for committing heartbeats for such sessions. If clients commit a new
 * session-based operation afterwards, they can create a new session.
 */
public class CloseInactiveSessionsOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private Collection<Long> sessions;

    public CloseInactiveSessionsOp() {
    }

    public CloseInactiveSessionsOp(Collection<Long> sessions) {
        this.sessions = sessions;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftSessionService service = getService();
        service.closeInactiveSessions(groupId, sessions);
        return null;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public String getServiceName() {
        return RaftSessionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftSessionServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftSessionServiceDataSerializerHook.CLOSE_INACTIVE_SESSIONS_OP;
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(sessions.size());
        for (long sessionId : sessions) {
            out.writeLong(sessionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int count = in.readInt();
        sessions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long sessionId = in.readLong();
            sessions.add(sessionId);
        }
    }
}
