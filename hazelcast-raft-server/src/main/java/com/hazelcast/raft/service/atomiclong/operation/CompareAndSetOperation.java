package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 */
public class CompareAndSetOperation extends AbstractAtomicLongOperation {

    private long currentValue;
    private long newValue;

    public CompareAndSetOperation() {
    }

    public CompareAndSetOperation(RaftGroupId groupId, long currentValue, long newValue) {
        super(groupId);
        this.currentValue = currentValue;
        this.newValue = newValue;
    }

    @Override
    public Object doRun(long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        return atomic.compareAndSet(currentValue, newValue, commitIndex);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(currentValue);
        out.writeLong(newValue);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentValue = in.readLong();
        newValue = in.readLong();
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.COMPARE_AND_SET_OP;
    }
}
