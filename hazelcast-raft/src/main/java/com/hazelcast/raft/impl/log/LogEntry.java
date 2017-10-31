package com.hazelcast.raft.impl.log;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftOperation;
import com.hazelcast.raft.impl.RaftDataSerializerHook;

import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class LogEntry implements IdentifiedDataSerializable {
    private int term;
    private int index;
    private RaftOperation operation;

    public LogEntry() {
    }

    public LogEntry(int term, int index, RaftOperation operation) {
        this.term = term;
        this.index = index;
        this.operation = operation;
    }

    public int index() {
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
        out.writeInt(index);
        out.writeObject(operation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        term = in.readInt();
        index = in.readInt();
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
