package com.hazelcast.raft.service.atomiclong;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.service.atomiclong.operation.AddAndGetOperation;
import com.hazelcast.raft.service.atomiclong.operation.AlterOperation;
import com.hazelcast.raft.service.atomiclong.operation.ApplyOperation;
import com.hazelcast.raft.service.atomiclong.operation.CompareAndSetOperation;
import com.hazelcast.raft.service.atomiclong.operation.GetAndAddOperation;
import com.hazelcast.raft.service.atomiclong.operation.GetAndSetOperation;
import com.hazelcast.raft.service.atomiclong.operation.LocalGetOperation;

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
                        return new AddAndGetOperation();
                    case COMPARE_AND_SET_OP:
                        return new CompareAndSetOperation();
                    case GET_AND_ADD_OP:
                        return new GetAndAddOperation();
                    case GET_AND_SET_OP:
                        return new GetAndSetOperation();
                    case ALTER_OP:
                        return new AlterOperation();
                    case APPLY_OP:
                        return new ApplyOperation();
                    case LOCAL_GET_OP:
                        return new LocalGetOperation();
                }
                throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
