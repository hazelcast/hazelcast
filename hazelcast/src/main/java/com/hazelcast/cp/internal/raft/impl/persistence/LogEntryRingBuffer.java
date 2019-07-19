package com.hazelcast.cp.internal.raft.impl.persistence;

public class LogEntryRingBuffer {
    private final long[] offsets;
    // A fixed formula determines for a given entry index its slot in offsets[].
    // bottomEntryIndex together with bufLen tells the range of entry indices
    // that are currently stored in offsets[]: [bottomEntryIndex .. bottomEntryIndex + bufLen - 1]
    private long bottomEntryIndex;
    private int bufLen;

    public LogEntryRingBuffer(int size, long topExistingIndex) {
        this.offsets = new long[size];
        this.bottomEntryIndex = topExistingIndex;
    }

    public long topIndex() {
        return bottomEntryIndex + bufLen - 1;
    }

    public void addEntryOffset(long offset) {
        int insertionPoint = (int) ((bottomEntryIndex + bufLen) % offsets.length);
        offsets[insertionPoint] = offset;
        if (bufLen == offsets.length) {
            bottomEntryIndex++;
        } else {
            bufLen++;
        }
    }

    public long getEntryOffset(long entryIndex) {
        if (entryIndex < bottomEntryIndex || entryIndex >= bottomEntryIndex + bufLen) {
            throw new IndexOutOfBoundsException(String.format(
                    "Asked for entry index %,d, available range is [%,d..%,d]",
                    entryIndex, bottomEntryIndex, bottomEntryIndex + bufLen - 1
            ));
        }
        int retrievalPoint = (int) ((entryIndex - bottomEntryIndex) % offsets.length);
        return offsets[retrievalPoint];
    }

    public void adjustToNewFile(long newStartOffset, long snapshotIndex) {
        if (bufLen == 0) {
            return;
        }
        long newBottomIndex = snapshotIndex + 1;
        if (newBottomIndex < bottomEntryIndex) {
            throw new IllegalArgumentException(String.format(
                    "Snapshot index %,d is less than the lowest entry index in the buffer %,d minus one",
                    snapshotIndex, bottomEntryIndex
            ));
        }
        int indexDelta = (int) (newBottomIndex - bottomEntryIndex);
        bufLen -= indexDelta;
        if (bufLen <= 0) {
            bufLen = 0;
            bottomEntryIndex = snapshotIndex;
            return;
        }
        long oldStartOffset = offsets[(int) (bottomEntryIndex % offsets.length)];
        long offsetDelta = newStartOffset - oldStartOffset;
        // We don't always have to update all entries, but this code is simpler at insignificant cost.
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] += offsetDelta;
        }
        bottomEntryIndex += indexDelta;
    }

    public long deleteEntriesFrom(long startIndex) {
        throw new UnsupportedOperationException("TODO");
    }
}
