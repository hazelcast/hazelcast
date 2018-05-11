package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractAtomicLongOp extends RaftOp implements IdentifiedDataSerializable {

    private String name;

    public AbstractAtomicLongOp() {
    }

    public AbstractAtomicLongOp(String name) {
        this.name = name;
    }

    protected RaftAtomicLong getAtomicLong(RaftGroupId groupId) {
        RaftAtomicLongService service = getService();
        return service.getAtomicLong(groupId, name);
    }

    @Override
    public final String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public final int getFactoryId() {
        return AtomicLongDataSerializerHook.F_ID;
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", name=").append(name);
    }
}
