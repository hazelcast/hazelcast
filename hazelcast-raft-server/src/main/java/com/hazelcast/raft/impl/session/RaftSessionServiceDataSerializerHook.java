/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.impl.session;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.session.operation.CloseInactiveSessionsOp;
import com.hazelcast.raft.impl.session.operation.CloseSessionOp;
import com.hazelcast.raft.impl.session.operation.ExpireSessionsOp;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.impl.session.operation.GetSessionsOp;
import com.hazelcast.raft.impl.session.operation.HeartbeatSessionOp;

@SuppressWarnings("checkstyle:declarationorder")
public class RaftSessionServiceDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_SESSION_DS_FACTORY_ID = -1003;
    private static final String RAFT_SESSION_DS_FACTORY = "hazelcast.serialization.ds.raft.session";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_SESSION_DS_FACTORY, RAFT_SESSION_DS_FACTORY_ID);

    public static final int SESSION = 1;
    public static final int SESSION_REGISTRY_SNAPSHOT = 2;
    public static final int SESSION_RESPONSE = 3;
    public static final int CREATE_SESSION = 4;
    public static final int HEARTBEAT_SESSION = 5;
    public static final int CLOSE_SESSION = 6;
    public static final int EXPIRE_SESSIONS = 7;
    public static final int CLOSE_INACTIVE_SESSIONS = 8;
    public static final int GET_SESSIONS = 9;


    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case SESSION:
                        return new Session();
                    case SESSION_REGISTRY_SNAPSHOT:
                        return new SessionRegistrySnapshot();
                    case SESSION_RESPONSE:
                        return new SessionResponse();
                    case CREATE_SESSION:
                        return new CreateSessionOp();
                    case HEARTBEAT_SESSION:
                        return new HeartbeatSessionOp();
                    case CLOSE_SESSION:
                        return new CloseSessionOp();
                    case EXPIRE_SESSIONS:
                        return new ExpireSessionsOp();
                    case CLOSE_INACTIVE_SESSIONS:
                        return new CloseInactiveSessionsOp();
                    case GET_SESSIONS:
                        return new GetSessionsOp();
                    default:
                        throw new IllegalArgumentException("Undefined type: " + typeId);
                }
            }
        };
    }
}
