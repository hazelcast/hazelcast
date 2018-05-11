package com.hazelcast.raft.impl.session;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.session.operation.CloseSessionOp;
import com.hazelcast.raft.impl.session.operation.InvalidateSessionsOp;
import com.hazelcast.raft.impl.session.operation.CreateSessionOp;
import com.hazelcast.raft.impl.session.operation.HeartbeatSessionOp;

public class RaftSessionServiceDataSerializerHook implements DataSerializerHook {
    private static final int RAFT_SESSION_DS_FACTORY_ID = -2011;
    private static final String RAFT_SESSION_DS_FACTORY = "hazelcast.serialization.ds.raft.session";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_SESSION_DS_FACTORY, RAFT_SESSION_DS_FACTORY_ID);

    public static final int SESSION_REGISTRY_SNAPSHOT = 1;
    public static final int SESSION_RESPONSE = 2;
    public static final int CREATE_SESSION = 3;
    public static final int HEARTBEAT_SESSION = 4;
    public static final int CLOSE_SESSION = 5;
    public static final int INVALIDATE_SESSIONS = 6;


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
                    case INVALIDATE_SESSIONS:
                        return new InvalidateSessionsOp();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
