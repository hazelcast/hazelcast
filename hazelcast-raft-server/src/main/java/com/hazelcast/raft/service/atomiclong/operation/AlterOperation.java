package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.service.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLong;

import java.io.IOException;

import static com.hazelcast.raft.service.atomiclong.operation.AlterOperation.AlterResultType.BEFORE_VALUE;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class AlterOperation extends AbstractAtomicLongOperation {

    public enum AlterResultType {
        BEFORE_VALUE,
        AFTER_VALUE
    }

    private IFunction<Long, Long> function;

    private AlterResultType alterResultType;

    public AlterOperation() {
    }

    public AlterOperation(RaftGroupId groupId, IFunction<Long, Long> function, AlterResultType alterResultType) {
        super(groupId);
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
        long before = atomic.getAndAdd(0, commitIndex);
        long after = function.apply(before);
        atomic.getAndSet(after, commitIndex);
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
