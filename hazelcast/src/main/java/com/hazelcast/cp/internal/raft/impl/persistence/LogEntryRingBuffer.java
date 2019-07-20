package com.hazelcast.cp.internal.raft.impl.persistence;

import static java.lang.Math.max;
import static java.lang.Math.min;

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

    /**
     * Called when receiving a snapshot and starting a new file with it. The
     * store puts the snapshot at the start of the new file and then copies all
     * the entries beyond the snapshot from the previous to the new file. This
     * method adjusts the file offsets of the copied entries and discards the
     * entries up to the snapshot.
     *
     * @param newStartOffset the start offset of the first entry to keep (snapshotIndex + 1)
     * @param snapshotIndex the snapshot's entry index
     */
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
        bottomEntryIndex += indexDelta;
        adjustOffsets(newStartOffset);
    }

    private void adjustOffsets(long newStartOffset) {
        long startIndexLong = bottomEntryIndex % offsets.length;
        long limitIndexLong = (startIndexLong + bufLen) % offsets.length;
        int startIndex = (int) startIndexLong;
        int limitIndex = (int) limitIndexLong;
        long offsetDelta = newStartOffset - offsets[startIndex];
        for (int i = startIndex; i != limitIndex; i++) {
            if (i == offsets.length) {
                i = 0;
            }
            offsets[i] += offsetDelta;
        }
    }

    public long deleteEntriesFrom(long deletionStartIndex) {
        try {
            return getEntryOffset(deletionStartIndex);
        } finally {
            long newBufLen = deletionStartIndex - bottomEntryIndex;
            bufLen = (int) (max(0, min(bufLen, newBufLen)));
        }
    }
}
