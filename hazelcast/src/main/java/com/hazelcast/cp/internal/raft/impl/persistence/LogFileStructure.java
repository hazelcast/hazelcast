package com.hazelcast.cp.internal.raft.impl.persistence;

import javax.annotation.Nonnull;

public class LogFileStructure {
    private final String filename;
    private final long[] tailEntryOffsets;
    private final long indexOfFirstTailEntry;

    public LogFileStructure(
            @Nonnull String filename,
            @Nonnull long[] tailEntryOffsets,
            long indexOfFirstTailEntry
    ) {
        this.filename = filename;
        this.tailEntryOffsets = tailEntryOffsets;
        this.indexOfFirstTailEntry = indexOfFirstTailEntry;
    }

    @Nonnull
    public String filename() {
        return filename;
    }

    @Nonnull
    public long[] tailEntryOffsets() {
        return tailEntryOffsets;
    }

    public long indexOfFirstTailEntry() {
        return indexOfFirstTailEntry;
    }
}
