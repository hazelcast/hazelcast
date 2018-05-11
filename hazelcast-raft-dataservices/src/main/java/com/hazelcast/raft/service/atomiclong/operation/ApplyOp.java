package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

public class ApplyOp<R> extends AbstractAtomicLongOp {

    private IFunction<Long, R> function;

    public ApplyOp() {
    }

    public ApplyOp(String name, IFunction<Long, R> function) {
        super(name);
        this.function = function;
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.APPLY_OP;
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong(groupId);
        long val = atomic.getAndAdd(0);
        return function.apply(val);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(function);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        function = in.readObject();
    }

}
