/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tstore;

import com.hazelcast.internal.memory.MemoryAllocator;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;

public final class Index {

    private static final int BUCKET_SCALE = 6;
    public static final int BUCKET_SIZE = 1 << BUCKET_SCALE;

    public interface BucketSupport {

        long allocate(long count);

        void freePrivate(long buckets, long count);

        // TODO: will be needed to support index resize (?)
        void freePublic(int threadIndex, long buckets, long count);

    }

    public interface RecordSupport {

        boolean keyEquals(long record, byte[] key, int keyOffset, int keyLength);

        long next(long record);

        void setNext(long record, long next);

        long create(byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset, int valueLength);

        void freePrivate(long record);

        void freePublic(int threadIndex, long record);

    }

    private static final long NULL = MemoryAllocator.NULL_ADDRESS;

    private static final int ENTRY_SIZE = Long.BYTES;
    private static final int OVERFLOW_ENTRY_INDEX = 7;
    private static final int OVERFLOW_ENTRY_OFFSET = OVERFLOW_ENTRY_INDEX * ENTRY_SIZE;

    private static final int ADDRESS_BITS = 48;
    private static final long ADDRESS_MASK = (1L << ADDRESS_BITS) - 1;

    // TODO: store flags in the lower bits?

    private static final int TAG_BITS = Long.SIZE - ADDRESS_BITS - 2;
    private static final int TAG_SHIFT = Long.SIZE - TAG_BITS;
    private static final long TAG_MASK = (1L << TAG_BITS) - 1 << TAG_SHIFT;

    private static final int TENTATIVE_SHIFT = TAG_SHIFT - 1;
    private static final long TENTATIVE_MASK = 1L << TENTATIVE_SHIFT;
    private static final long TAG_AND_TENTATIVE_MASK = TAG_MASK | TENTATIVE_MASK;

    private static final int LATCH_SHIFT = TAG_SHIFT - 2;
    private static final long LATCH_MASK = 1L << LATCH_SHIFT;

    private final Epoch epoch;
    private final State state;
    private final BucketSupport bucketSupport;
    private final RecordSupport recordSupport;

    private final Table[] tables = new Table[]{new Table(), new Table()};
    private volatile int version;

    public Index(Epoch epoch, State state, BucketSupport bucketSupport, RecordSupport recordSupport) {
        this.epoch = epoch;
        this.state = state;
        this.bucketSupport = bucketSupport;
        this.recordSupport = recordSupport;

        this.tables[0].allocate(Table.DEFAULT_BITS, bucketSupport);
        this.version = 0;
    }

    public long get(int threadIndex, long keyHash, byte[] key, int keyOffset, int keyLength) {
        Table table = tables[version];
        long index = keyHash & table.mask;
        long entry = table.buckets + (index << BUCKET_SCALE);
        long tag = tag(keyHash);

        do {
            for (int i = 0; i < OVERFLOW_ENTRY_INDEX; ++i) {
                long record = read(entry);

                if (record != NULL && tagAndTentative(record) == tag) {
                    do {
                        record = address(record);
                        if (recordSupport.keyEquals(record, key, keyOffset, keyLength)) {
                            return record;
                        }

                        record = recordSupport.next(record);
                    } while (record != NULL);

                    return NULL;
                }

                entry += ENTRY_SIZE;
            }

            entry = read(entry);
        } while (entry != NULL);

        return NULL;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "UnnecessaryLabelOnContinueStatement", "checkstyle:MethodLength",
            "checkstyle:NPathComplexity"})
    public long put(int threadIndex, long keyHash, byte[] key, int keyOffset, int keyLength, byte[] value, int valueOffset,
                    int valueLength) {
        long newRecord = recordSupport.create(key, keyOffset, keyLength, value, valueOffset, valueLength);

        Table table = tables[version];
        long index = keyHash & table.mask;
        long bucket = table.buckets + (index << BUCKET_SCALE);
        long tag = tag(keyHash);

        retry:
        while (true) {
            long entry = bucket;
            long record = NULL;

            // Try to find a non-tentative entry for the tag.

            findTag:
            do {
                for (int i = 0; i < OVERFLOW_ENTRY_INDEX; ++i) {
                    record = read(entry);
                    if (record != NULL && tagAndTentative(record) == tag) {
                        break findTag;
                    }
                    entry += ENTRY_SIZE;
                }
                entry = read(entry);
            } while (entry != NULL);

            // Add an entry for the tag if not found.

            if (entry == NULL) {
                entry = bucket;
                do {
                    for (int i = 0; i < OVERFLOW_ENTRY_INDEX; ++i) {
                        record = read(entry);
                        if (record != NULL) {
                            entry += ENTRY_SIZE;
                            continue;
                        }

                        if (!cas(entry, NULL, tentativeRecord(tag, newRecord))) {
                            entry += ENTRY_SIZE;
                            continue;
                        }

                        if (anotherEntryExists(entry, bucket, tag)) {
                            // release the current one
                            write(entry, NULL);
                            // TODO: spin wait or Thread.yield() or something?
                            continue retry;
                        }

                        // mark as non-tentative
                        write(entry, record(tag, newRecord));
                        return NULL;
                    }

                    entry = read(entry);
                } while (entry != NULL);

                // No free entry found, allocate a new bucket and retry.

                // TODO: insert the new bucket at the tail, not at the head?
                long newBucket = bucketSupport.allocate(1);
                long overflowEntry = bucket + OVERFLOW_ENTRY_OFFSET;
                long next = read(overflowEntry);
                write(newBucket + OVERFLOW_ENTRY_OFFSET, next);
                if (!cas(overflowEntry, next, newBucket)) {
                    // some other thread has inserted a new bucket
                    bucketSupport.freePrivate(newBucket, 1);
                }

                continue retry;
            }

            // Now we know a non-tentative entry for an existing tag,
            // latch it.

            long first = latchEntry(entry, record, tag);
            if (first == NULL) {
                continue retry;
            }

            long previous = NULL;
            long current = first;
            do {
                current = address(current);

                if (recordSupport.keyEquals(current, key, keyOffset, keyLength)) {
                    // Replace the existing record.

                    long next = address(recordSupport.next(current));
                    recordSupport.setNext(newRecord, next);
                    if (previous == NULL) {
                        // also releases the latch
                        write(entry, record(tag, newRecord));
                    } else {
                        recordSupport.setNext(previous, newRecord);
                        // release the latch
                        write(entry, first);
                    }

                    recordSupport.freePublic(threadIndex, current);
                    return current;
                }

                previous = current;
                current = recordSupport.next(current);
            } while (current != NULL);

            // Insert the new record.

            recordSupport.setNext(previous, newRecord);
            // release the latch
            write(entry, first);
            return NULL;
        }
    }

    @SuppressWarnings("UnnecessaryLabelOnContinueStatement")
    public long remove(int threadIndex, long keyHash, byte[] key, int keyOffset, int keyLength) {
        Table table = tables[version];
        long index = keyHash & table.mask;
        long bucket = table.buckets + (index << BUCKET_SCALE);
        long tag = tag(keyHash);

        retry:
        while (true) {
            long entry = bucket;
            long record;

            // Try to find a non-tentative entry for the tag.

            findTag:
            while (true) {
                for (int i = 0; i < OVERFLOW_ENTRY_INDEX; ++i) {
                    record = read(entry);
                    if (record != NULL && tagAndTentative(record) == tag) {
                        break findTag;
                    }
                    entry += ENTRY_SIZE;
                }

                entry = read(entry);
                if (entry == NULL) {
                    return NULL;
                }
            }

            // Now we know a non-tentative entry for an existing tag,
            // latch it.

            long first = latchEntry(entry, record, tag);
            if (first == NULL) {
                continue retry;
            }

            long previous = NULL;
            long current = first;
            do {
                current = address(current);

                if (recordSupport.keyEquals(current, key, keyOffset, keyLength)) {
                    // Key found.

                    long next = address(recordSupport.next(current));
                    if (previous == NULL) {
                        // also releases the latch
                        write(entry, next == NULL ? NULL : record(tag, next));
                    } else {
                        recordSupport.setNext(previous, next);
                        // release the latch
                        write(entry, first);
                    }

                    recordSupport.freePublic(threadIndex, current);
                    return current;
                }

                previous = current;
                current = recordSupport.next(current);
            } while (current != NULL);

            // Key not found.

            // release the latch
            write(entry, first);
            return NULL;
        }
    }

    public void close() {
        tables[version].free(bucketSupport, recordSupport);
    }

    private static boolean anotherEntryExists(long excludedEntry, long bucket, long tag) {
        long entry = bucket;
        do {
            for (int i = 0; i < OVERFLOW_ENTRY_INDEX; ++i) {
                long record = read(entry);
                if (record != NULL && tag(record) == tag && entry != excludedEntry) {
                    return true;
                }
                entry += ENTRY_SIZE;
            }

            entry = read(entry);
        } while (entry != NULL);

        return false;
    }

    private static long latchEntry(long entry, long record, long tag) {
        assert (record & TENTATIVE_MASK) == 0;

        long unlatchedRecord = unlatchedRecord(record);
        while (!cas(entry, unlatchedRecord, latchedRecord(record))) {
            record = read(entry);
            if (record == NULL || tagAndTentative(record) != tag) {
                // tag has been removed or changed
                return NULL;
            }
            unlatchedRecord = unlatchedRecord(record);
        }

        return unlatchedRecord;
    }

    private static long tag(long hash) {
        return hash & TAG_MASK;
    }

    private static long tagAndTentative(long record) {
        return record & TAG_AND_TENTATIVE_MASK;
    }

    private static long address(long record) {
        return record & ADDRESS_MASK;
    }

    private static long tentativeRecord(long tag, long address) {
        return tag | TENTATIVE_MASK | address;
    }

    private static long record(long tag, long address) {
        return tag | address;
    }

    private static long latchedRecord(long record) {
        return LATCH_MASK | record;
    }

    private static long unlatchedRecord(long record) {
        return ~LATCH_MASK & record;
    }

    private static long read(long address) {
        return AMEM.getLongVolatile(address);
    }

    private static void write(long address, long value) {
        AMEM.putLongVolatile(address, value);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean cas(long address, long expected, long value) {
        return AMEM.compareAndSwapLong(address, expected, value);
    }

    private static final class Table {

        static final int DEFAULT_BITS = 1;

        int bits;
        long mask;
        long buckets;

        void allocate(int bits, BucketSupport bucketSupport) {
            assert bits >= 0 && bits <= Long.SIZE;

            this.bits = bits;
            long capacity = 1L << bits;
            this.mask = capacity - 1;
            this.buckets = bucketSupport.allocate(capacity);
        }

        void free(BucketSupport bucketSupport, RecordSupport recordSupport) {
            long capacity = 1L << bits;

            long endBucket = buckets + capacity * BUCKET_SIZE;
            for (long bucket = buckets; bucket < endBucket; bucket += BUCKET_SIZE) {
                long entry = bucket;
                long overflowBucket = NULL;

                do {
                    for (int i = 0; i < OVERFLOW_ENTRY_INDEX; ++i) {
                        long record = read(entry);

                        while (record != NULL) {
                            record = address(record);
                            long nextRecord = recordSupport.next(record);
                            recordSupport.freePrivate(record);
                            record = nextRecord;
                        }

                        entry += ENTRY_SIZE;
                    }

                    entry = read(entry);

                    if (overflowBucket != NULL) {
                        bucketSupport.freePrivate(overflowBucket, 1);
                    }
                    overflowBucket = entry;
                } while (entry != NULL);
            }

            bucketSupport.freePrivate(buckets, capacity);
        }

    }

}
