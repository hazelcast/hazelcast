package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

import static com.hazelcast.raft.service.atomiclong.operation.AlterOp.AlterResultType.BEFORE_VALUE;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class AlterOp extends AbstractAtomicLongOp {

    public enum AlterResultType {
        BEFORE_VALUE,
        AFTER_VALUE
    }

    private IFunction<Long, Long> function;

    private AlterResultType alterResultType;

    public AlterOp() {
    }

    public AlterOp(RaftGroupId groupId, String name, IFunction<Long, Long> function, AlterResultType alterResultType) {
        super(groupId, name);
        checkNotNull(alterResultType);
        this.function = function;
        this.alterResultType = alterResultType;
    }

    @Override
    public int getId() {
        return AtomicLongDataSerializerHook.ALTER_OP;
    }

    @Override
    protected Object doRun(long commitIndex) {
        RaftAtomicLong atomic = getAtomicLong();
        long before = atomic.getAndAdd(0);
        long after = function.apply(before);
        atomic.getAndSet(after);
        return alterResultType == BEFORE_VALUE ? before : after;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(function);
        out.writeUTF(alterResultType.name());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        function = in.readObject();
        alterResultType = AlterResultType.valueOf(in.readUTF());
    }

}
