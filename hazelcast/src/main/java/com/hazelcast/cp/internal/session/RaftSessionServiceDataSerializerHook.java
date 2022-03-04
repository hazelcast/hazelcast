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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cp.internal.session.operation.CloseInactiveSessionsOp;
import com.hazelcast.cp.internal.session.operation.CloseSessionOp;
import com.hazelcast.cp.internal.session.operation.CreateSessionOp;
import com.hazelcast.cp.internal.session.operation.ExpireSessionsOp;
import com.hazelcast.cp.internal.session.operation.GenerateThreadIdOp;
import com.hazelcast.cp.internal.session.operation.GetSessionsOp;
import com.hazelcast.cp.internal.session.operation.HeartbeatSessionOp;
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public class RaftSessionServiceDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_SESSION_DS_FACTORY_ID = -1003;
    private static final String RAFT_SESSION_DS_FACTORY = "hazelcast.serialization.ds.raft.session";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_SESSION_DS_FACTORY, RAFT_SESSION_DS_FACTORY_ID);

    public static final int RAFT_SESSION = 1;
    public static final int RAFT_SESSION_REGISTRY = 2;
    public static final int SESSION_RESPONSE = 3;
    public static final int CREATE_SESSION_OP = 4;
    public static final int HEARTBEAT_SESSION_OP = 5;
    public static final int CLOSE_SESSION_OP = 6;
    public static final int EXPIRE_SESSIONS_OP = 7;
    public static final int CLOSE_INACTIVE_SESSIONS_OP = 8;
    public static final int GET_SESSIONS_OP = 9;
    public static final int GENERATE_THREAD_ID_OP = 10;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case RAFT_SESSION:
                    return new CPSessionInfo();
                case RAFT_SESSION_REGISTRY:
                    return new RaftSessionRegistry();
                case SESSION_RESPONSE:
                    return new SessionResponse();
                case CREATE_SESSION_OP:
                    return new CreateSessionOp();
                case HEARTBEAT_SESSION_OP:
                    return new HeartbeatSessionOp();
                case CLOSE_SESSION_OP:
                    return new CloseSessionOp();
                case EXPIRE_SESSIONS_OP:
                    return new ExpireSessionsOp();
                case CLOSE_INACTIVE_SESSIONS_OP:
                    return new CloseInactiveSessionsOp();
                case GET_SESSIONS_OP:
                    return new GetSessionsOp();
                case GENERATE_THREAD_ID_OP:
                    return new GenerateThreadIdOp();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
