package com.hazelcast.raft.impl.log;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.impl.RaftDataSerializerHook;
import com.hazelcast.raft.operation.RaftOperation;

import java.io.IOException;

/**
 * Represents an entry in the {@link RaftLog}.
 * <p>
 * Each log entry stores a state machine command ({@link RaftOperation}) along with the term number
 * when the entry was received by the leader. The term numbers in log entries are used to detect inconsistencies
 * between logs. Each log entry also has an integer index identifying its position in the log.
 */
public class LogEntry implements IdentifiedDataSerializable {
    private int term;
    private long index;
    private RaftOperation operation;

    public LogEntry() {
    }

    public LogEntry(int term, long index, RaftOperation operation) {
        this.term = term;
        this.index = index;
        this.operation = operation;
    }

    public long index() {
        return index;
    }

    public int term() {
        return term;
    }

    public RaftOperation operation() {
        return operation;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(term);
        out.writeLong(index);
        out.writeObject(operation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        index = in.readLong();
        operation = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftDataSerializerHook.LOG_ENTRY;
    }

    @Override
    public String toString() {
        return "LogEntry{" + "term=" + term + ", index=" + index + ", operation=" + operation + '}';
    }
}
