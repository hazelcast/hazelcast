package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

import java.io.Closeable;
import java.io.IOException;

/**
 * TODO: Javadoc Pending...
 *
 */
public interface RaftLogStore extends Closeable {

    void appendEntry(LogEntry entry) throws IOException;

    void flush() throws IOException;

    void writeSnapshot(SnapshotEntry entry) throws IOException;

    // TODO [basri] I think we can have an upper-bound for how many entries to truncate...
    void truncateEntriesFrom(long indexInclusive) throws IOException;

}
