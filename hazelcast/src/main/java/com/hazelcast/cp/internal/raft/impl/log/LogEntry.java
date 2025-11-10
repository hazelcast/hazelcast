/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.raft.impl.log;

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Represents an entry in the {@code RaftLog}.
 * Each log entry stores a state machine command along with the term number
 * when the entry was received by the leader. The term numbers in log entries
 * are used to detect inconsistencies between logs. Each log entry also has
 * an integer index identifying its position in the log.
 */
public class LogEntry implements IdentifiedDataSerializable {

    private int term;
    private long index;
    private Object operation;

    public LogEntry() {
    }

    public LogEntry(int term, long index, Object operation) {
        this.term = term;
        this.index = index;
        this.operation = operation;
    }

    public LogEntry setSnapshotTerm(int term) {
        this.term = term;
        return this;
    }

    public LogEntry setIndex(long index) {
        this.index = index;
        return this;
    }

    public LogEntry setOperation(Object operation) {
        this.operation = operation;
        return this;
    }

    public long index() {
        return index;
    }

    public int term() {
        return term;
    }

    public Object operation() {
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
        return RaftDataSerializerConstants.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftDataSerializerConstants.LOG_ENTRY;
    }

    @Override
    public String toString() {
        return "LogEntry{" + "term=" + term + ", index=" + index + ", operation=" + operation + '}';
    }
}
