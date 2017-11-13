package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

public class ApplyOperation<R> extends AbstractAtomicLongOperation {

    private IFunction<Long, R> function;

    public ApplyOperation() {
    }

    public ApplyOperation(RaftGroupId groupId, IFunction<Long, R> function) {
        super(groupId);
        this.function = function;
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.APPLY_OP;
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        long val = atomic.getAndAdd(0, commitIndex);
        return function.apply(val);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(function);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readObject();
    }

}
