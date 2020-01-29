/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.logstore;

import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.log.CloseableIterator;
import com.hazelcast.log.encoders.Consumed;
import com.hazelcast.log.encoders.Encoder;
import sun.misc.Unsafe;

import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class LogStore<E> {

    protected static final Unsafe UNSAFE = UnsafeUtil.UNSAFE;

    protected final LogStoreConfig config;
    protected final Eden eden;
    protected final MemoryAllocator allocator;
    protected final Encoder<E> encoder;
    protected final int segmentSize;
    protected final int maxSegmentCount;
    protected final Class type;
    protected final long tenuringAgeNanos;
    protected final long retentionNanos;
    protected Segment first;
    protected Segment last;
    protected int segmentCount;

    public LogStore(LogStoreConfig config) {
        this.config = config;
        this.encoder = config.getEncoder();
        this.allocator = config.getAllocator();
        this.eden = new Eden();
        this.segmentSize = config.getSegmentSize();
        this.type = config.getType();
        this.maxSegmentCount = config.getMaxSegmentCount();
        this.tenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());
        this.retentionNanos = config.getRetentionMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getRetentionMillis());

    }

    public static LogStore create(LogStoreConfig config) {
        Class type = config.getType();
        if (Byte.TYPE.equals(type)) {
            return new ByteLogStore(config);
        } else if (Integer.TYPE.equals(type)) {
            return new IntLogStore(config);
        } else if (Long.TYPE.equals(type)) {
            return new LongLogStore(config);
        } else if (Double.TYPE.equals(type)) {
            return new DoubleLogStore(config);
        } else {
            return new ObjectLogStore(config);
        }
    }

    protected void updateEdenTime() {
        if (eden.count == 0) {
            long now = System.nanoTime();
            eden.fromNanos = now;
            eden.toNanos = now;
        } else {
            eden.toNanos = System.nanoTime();
        }
    }

    public void touch() {
        long now = System.nanoTime();

        if (tenuringAgeNanos != Long.MAX_VALUE) {
            if (eden.count != 0 && now > eden.fromNanos + tenuringAgeNanos) {
                System.out.println("Tenure Eden");
                tenureEden();
            }
        }

        if (retentionNanos != Long.MAX_VALUE) {
            while (last != null) {
                //todo: verify logic for overflow
                if (now > last.toNanos + retentionNanos) {
                    System.out.println("Drop Last Region");
                    dropOldestRegion();
                } else {
                    break;
                }
            }
        }
    }

    public LogStoreConfig config() {
        return config;
    }

    public int segmentCount() {
        return segmentCount;
    }

    // instead of doing a linear scan over the segment, we should put them in a tree.
    // so we get an O(N) complexity
    private Segment getSegment(long sequence) {
        Segment segment = first;
        while (segment != null) {
            if (segment.startSequence <= sequence && segment.startSequence + segment.limit >= sequence) {
                return segment;
            } else {
                segment = segment.next;
            }
        }

        throw new IllegalStateException("Can't find segment for sequence:" + sequence);
    }

    protected long toAddress(long sequence) {
        if (eden.startSequence <= sequence) {
            long offset = sequence - eden.startSequence;
            return eden.address + offset;
        } else {
            Segment segment = getSegment(sequence);
            long offset = sequence - segment.startSequence;
            return segment.address + offset;
        }
    }

    public abstract long putObject(E o);

    public abstract E getObject(long sequence);

    protected void ensureCapacity(int required) {
        if (required > segmentSize) {
            throw new IllegalArgumentException("required " + required + " can't be larger than segmentSize:" + segmentSize);
        }

        if (eden.address == 0) {
            // deal with a lazily created eden
            eden.address = allocator.allocate(segmentSize);
            return;
        }

        int remaining = segmentSize - eden.position;
        if (remaining >= required) {
            return;
        }

        tenureEden();

        if (eden.address == 0) {
            eden.address = allocator.allocate(segmentSize);
        }
    }

    private void tenureEden() {
        long address = eden.address;
        Segment segment = new Segment(address, eden.position, eden.startSequence, eden.count, eden.fromNanos, eden.toNanos);

        if (first == null) {
            last = segment;
            first = segment;
        } else {
            Segment tmp = first;
            tmp.prev = segment;
            first = segment;
            segment.next = tmp;
        }
        segmentCount++;

        if (segmentCount > maxSegmentCount) {
            dropOldestRegion();
        }

        eden.address = 0;
        eden.startSequence += eden.position;
        eden.position = 0;
        eden.count = 0;
        eden.fromNanos = 0;
        eden.toNanos = 0;
    }

    private void dropOldestRegion() {
        last.release();
        if (last == first) {
            first = null;
            last = null;
        } else {
            this.last = last.prev;
            last.next = null;
        }
        segmentCount--;
    }

    public void clear() {
        if (eden.address != 0) {
            allocator.free(eden.address, segmentSize);
            eden.address = 0;
            eden.position = 0;
            eden.count = 0;
        }

        Segment segment = last;
        while (segment != null) {
            if (segment.address != 0) {
                segment.release();
            }
            segment = segment.prev;
        }
        segmentCount = 0;
        first = null;
        last = null;
    }

    // todo: convert to O(1)
    public long bytesInUse() {
        long used = eden.position;
        Segment segment = first;
        while (segment != null) {
            used += segment.limit;
            segment = segment.next;
        }
        return used;
    }

    public long bytesAllocated() {
        long allocated = 0;
        if (eden.address != 0) {
            allocated += segmentSize;
        }
        allocated += segmentCount * segmentSize;
        return allocated;
    }

    // todo: convert to O(1)
    public long count() {
        long count = eden.count;
        Segment segment = first;
        while (segment != null) {
            count += segment.count;
            segment = segment.next;
        }
        return count;
    }

    public abstract E reduce(BinaryOperator<E> accumulator);

    public CloseableIterator iterator() {
        Segment segment = first;
        Segment[] segments = new Segment[segmentCount];
        System.out.println(segments.length);
        for (int k = 0; k < segments.length; k++) {
            segments[k] = segment;
            segment.acquire();
            segment = segment.next;
        }
        return new CloseableIteratorImpl(segments, encoder);
    }

    // todo: we are missing the items in eden.
    private static class CloseableIteratorImpl implements CloseableIterator {
        private final Encoder encoder;
        private final Consumed consumed = new Consumed();
        private final Segment[] segments;
        private int segmentIndex;
        private int position;
        private Object item;

        public CloseableIteratorImpl(Segment[] segments, Encoder encoder) {
            this.segments = segments;
            this.encoder = encoder;
            this.segmentIndex = segments.length - 1;
        }

        @Override
        public void close() {
            while (segmentIndex >= 0) {
                segments[segmentIndex].release();
                segments[segmentIndex] = null;
                segmentIndex--;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (item != null) {
                return true;
            }

            while (segmentIndex >= 0) {
                Segment segment = segments[segmentIndex];
                consumed.value = 0;
                if (position < segment.limit) {
                    item = encoder.decode(segment.address + position, consumed);
                    if (item != null) {
                        position += consumed.value;
                        return true;
                    }
                }

                segment.release();
                position = 0;
                segmentIndex--;
            }

            return false;
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Object tmp = item;
            item = null;
            return tmp;
        }
    }
}
