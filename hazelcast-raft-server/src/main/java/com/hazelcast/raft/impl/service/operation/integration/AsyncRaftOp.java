package com.hazelcast.raft.impl.service.operation.integration;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.RaftSystemOperation;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.raft.impl.service.RaftServiceDataSerializerHook;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
abstract class AsyncRaftOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    protected RaftGroupId groupId;

    public AsyncRaftOp() {
    }

    public AsyncRaftOp(RaftGroupId groupId) {
        this.groupId = groupId;
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    // Raft ops are executed on partition threads but not sent to partition owners.
    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }
}
