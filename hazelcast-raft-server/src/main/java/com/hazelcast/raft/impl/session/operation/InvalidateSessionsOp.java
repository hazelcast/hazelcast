/*
 *  Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.session.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.impl.session.RaftSessionService;
import com.hazelcast.raft.impl.session.RaftSessionServiceDataSerializerHook;
import com.hazelcast.raft.impl.util.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TODO: Javadoc Pending...
 */
public class InvalidateSessionsOp extends RaftOp implements IdentifiedDataSerializable {

    private Collection<Tuple2<Long, Long>> sessions;

    public InvalidateSessionsOp() {
    }

    public InvalidateSessionsOp(Collection<Tuple2<Long, Long>> sessionIds) {
        this.sessions = sessionIds;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftSessionService service = getService();
        service.invalidateSessions(groupId, sessions);
        return null;
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
    public int getId() {
        return RaftSessionServiceDataSerializerHook.INVALIDATE_SESSIONS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(sessions.size());
        for (Tuple2<Long, Long> s : sessions) {
            out.writeLong(s.element1);
            out.writeLong(s.element2);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        List<Tuple2<Long, Long>> sessionIds = new ArrayList<Tuple2<Long, Long>>();
        for (int i = 0; i < size; i++) {
            long sessionId = in.readLong();
            long version = in.readLong();
            sessionIds.add(Tuple2.of(sessionId, version));
        }
        this.sessions = sessionIds;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", sessions=").append(sessions);
    }
}
