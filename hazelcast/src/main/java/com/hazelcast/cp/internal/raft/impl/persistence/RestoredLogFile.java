package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;

public class RestoredLogFile {
    private final long topIndex;
    private String filename;
    private final SnapshotEntry snapshotEntry;
    private final LogEntry[] entries;

    public RestoredLogFile(String filename, SnapshotEntry snapshotEntry, LogEntry[] entries, long topIndex) {
        this.filename = filename;
        this.snapshotEntry = snapshotEntry;
        this.entries = entries;
        this.topIndex = topIndex;
    }

    public long topIndex() {
        return topIndex;
    }

    public SnapshotEntry snapshotEntry() {
        return snapshotEntry;
    }

    public LogEntry[] entries() {
        return entries;
    }

    public String filename() {
        return filename;
    }
}
