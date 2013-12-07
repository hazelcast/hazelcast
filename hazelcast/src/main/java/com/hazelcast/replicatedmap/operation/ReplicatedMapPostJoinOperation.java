package com.hazelcast.replicatedmap.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.ReplicatedMapDataSerializerHook;
import com.hazelcast.replicatedmap.ReplicatedMapService;
import com.hazelcast.replicatedmap.record.AbstractReplicatedRecordStorage;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class ReplicatedMapPostJoinOperation
        extends AbstractReplicatedMapOperation
        implements IdentifiedDataSerializable {

    private String[] replicatedMaps;
    private int chunkSize;

    public ReplicatedMapPostJoinOperation() {
    }

    public ReplicatedMapPostJoinOperation(String[] replicatedMaps, int chunkSize) {
        this.replicatedMaps = replicatedMaps;
        this.chunkSize = chunkSize;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService replicatedMapService = getService();
        for (String replicatedMap : replicatedMaps) {
            AbstractReplicatedRecordStorage replicatedRecordStorage =
                    (AbstractReplicatedRecordStorage) replicatedMapService.getReplicatedRecordStore(replicatedMap);

            replicatedRecordStorage.queueInitialFillup(getCallerAddress(), chunkSize);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(chunkSize);
        out.writeInt(replicatedMaps.length);
        for (int i = 0; i < replicatedMaps.length; i++) {
            out.writeUTF(replicatedMaps[i]);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        chunkSize = in.readInt();
        int length = in.readInt();
        replicatedMaps = new String[length];
        for (int i = 0; i < length; i++) {
            replicatedMaps[i] = in.readUTF();
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.OP_POST_JOIN;
    }

}
