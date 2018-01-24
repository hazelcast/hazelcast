package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.atomiclong.operation.AddAndGetOp;
import com.hazelcast.raft.service.atomiclong.operation.AlterOp;
import com.hazelcast.raft.service.atomiclong.operation.ApplyOp;
import com.hazelcast.raft.service.atomiclong.operation.CompareAndSetOp;
import com.hazelcast.raft.service.atomiclong.operation.GetAndAddOp;
import com.hazelcast.raft.service.atomiclong.operation.GetAndSetOp;
import com.hazelcast.raft.service.atomiclong.operation.LocalGetOp;

public final class AtomicLongDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_ATOMIC_LONG_DS_FACTORY_ID = -1011;
    private static final String RAFT_ATOMIC_LONG_DS_FACTORY = "hazelcast.serialization.ds.raft.atomiclong";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_ATOMIC_LONG_DS_FACTORY, RAFT_ATOMIC_LONG_DS_FACTORY_ID);

    public static final int ADD_AND_GET_OP = 1;
    public static final int COMPARE_AND_SET_OP = 2;
    public static final int GET_AND_ADD_OP = 3;
    public static final int GET_AND_SET_OP = 4;
    public static final int ALTER_OP = 5;
    public static final int APPLY_OP = 6;
    public static final int LOCAL_GET_OP = 7;

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
                    case ADD_AND_GET_OP:
                        return new AddAndGetOp();
                    case COMPARE_AND_SET_OP:
                        return new CompareAndSetOp();
                    case GET_AND_ADD_OP:
                        return new GetAndAddOp();
                    case GET_AND_SET_OP:
                        return new GetAndSetOp();
                    case ALTER_OP:
                        return new AlterOp();
                    case APPLY_OP:
                        return new ApplyOp();
                    case LOCAL_GET_OP:
                        return new LocalGetOp();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
