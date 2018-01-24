package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftOp;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongService;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public abstract class AbstractAtomicLongOp extends RaftOp implements IdentifiedDataSerializable {

    private RaftGroupId groupId;
    private String name;

    public AbstractAtomicLongOp() {
    }

    public AbstractAtomicLongOp(RaftGroupId groupId, String name) {
        this.groupId = groupId;
        this.name = name;
    }

    protected RaftAtomicLong getAtomicLong() {
        RaftAtomicLongService service = getService();
        return service.getAtomicLong(groupId, name);
    }

    @Override
    public final String getServiceName() {
        return RaftAtomicLongService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        name = in.readUTF();
    }

    @Override
    public final int getFactoryId() {
        return AtomicLongDataSerializerHook.F_ID;
    }
}
