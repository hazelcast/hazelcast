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
import com.hazelcast.internal.util.BiTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Expires sessions that do have not committed any heartbeat for
 * {@link CPSubsystemConfig#getSessionTimeToLiveSeconds()} seconds.
 */
public class ExpireSessionsOp extends RaftOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    private Collection<BiTuple<Long, Long>> sessions;

    public ExpireSessionsOp() {
    }

    public ExpireSessionsOp(Collection<BiTuple<Long, Long>> sessionIds) {
        this.sessions = sessionIds;
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        RaftSessionService service = getService();
        service.expireSessions(groupId, sessions);
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
        return RaftSessionServiceDataSerializerHook.EXPIRE_SESSIONS_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(sessions.size());
        for (BiTuple<Long, Long> s : sessions) {
            out.writeLong(s.element1);
            out.writeLong(s.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        List<BiTuple<Long, Long>> sessionIds = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            long sessionId = in.readLong();
            long version = in.readLong();
            sessionIds.add(BiTuple.of(sessionId, version));
        }
        this.sessions = sessionIds;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", sessions=").append(sessions);
    }
}
