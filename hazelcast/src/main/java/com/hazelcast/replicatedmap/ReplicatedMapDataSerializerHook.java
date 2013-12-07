package com.hazelcast.replicatedmap;

import com.hazelcast.nio.serialization.*;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.operation.ReplicatedMapInitChunkOperation;
import com.hazelcast.replicatedmap.operation.ReplicatedMapPostJoinOperation;
import com.hazelcast.replicatedmap.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.record.Vector;
import com.hazelcast.util.ConstructorFunction;

public class ReplicatedMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.REPLICATED_MAP_DS_FACTORY, -22);

    public static final int VECTOR = 0;
    public static final int RECORD = 1;
    public static final int REPL_UPDATE_MESSAGE = 2;
    public static final int REPL_CLEAR_MESSAGE = 3;
    public static final int OP_INIT_CHUNK = 4;
    public static final int OP_POST_JOIN = 5;

    private static final int LEN = OP_POST_JOIN + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[LEN];
        constructors[VECTOR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Vector();
            }
        };
        constructors[RECORD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedRecord();
            }
        };
        constructors[REPL_UPDATE_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicationMessage();
            }
        };
        constructors[REPL_CLEAR_MESSAGE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new Vector();
            }
        };
        constructors[OP_INIT_CHUNK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapInitChunkOperation();
            }
        };
        constructors[OP_POST_JOIN] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ReplicatedMapPostJoinOperation();
            }
        };

        return new ArrayDataSerializableFactory(constructors);
    }
}
