/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.InMemoryFormat.values;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * The RingbufferContainer is responsible for storing the actual content of a ringbuffer.
 *
 * Currently the Ringbuffer is not a partitioned data-structure. So all data of a ringbuffer is stored in a single partition
 * and replicated to the replica's. No thread-safety is needed since a partition can only be accessed by a single thread at
 * any given moment.
 *
 * The ringItems is the ring that contains the actual items.
 * The ringExpiration contains the expiration time of an item.
 * The if a time to live is set, the ringExpiration is created. Otherwise it is null to safe space.
 * The expiration time of an item can be found at the same index as the item itself. So these 2 arrays are always in step with
 * each other.
 * The reason why 2 array are created instead of just wrapping the item in a new object containing the expiration is that
 * we don't want to generate more waste than needed.
 */
public class RingbufferContainer implements DataSerializable {

    private static final long TTL_DISABLED = 0;

    // contains the actual items
    Object[] ringItems;
    // contains the expiration time in ms when the item should be expired.
    long[] ringExpirationMs;

    InMemoryFormat inMemoryFormat;
    long ttlMs;
    RingbufferConfig config;
    String name;
    long tailSequence = -1;
    long headSequence = tailSequence + 1;
    int capacity;

    // a cached version of the wait notify key needed to wait for a change if the ringbuffer is empty
    private final RingbufferWaitNotifyKey emptyRingWaitNotifyKey;
    private SerializationService serializationService;

    public RingbufferContainer(String name) {
        this.name = name;
        this.emptyRingWaitNotifyKey = new RingbufferWaitNotifyKey(name, "empty");
    }

    public RingbufferContainer(RingbufferConfig config, SerializationService serializationService) {
       this(config.getName(), config, serializationService);
    }

    public RingbufferContainer(String name, RingbufferConfig config, SerializationService serializationService) {
        this(name);
        this.serializationService = serializationService;
        this.config = config;
        this.capacity = config.getCapacity();
        this.inMemoryFormat = config.getInMemoryFormat();
        this.ringItems = new Object[capacity];
        this.ttlMs = SECONDS.toMillis(config.getTimeToLiveSeconds());

        if (isTTLEnabled()) {
            ringExpirationMs = new long[capacity];
        }
    }

    public void init(NodeEngine nodeEngine) {
        this.config = nodeEngine.getConfig().getRingbufferConfig(name);
        this.serializationService = nodeEngine.getSerializationService();
    }

    public RingbufferWaitNotifyKey getRingEmptyWaitNotifyKey() {
        return emptyRingWaitNotifyKey;
    }

    public RingbufferConfig getConfig() {
        return config;
    }

    public long tailSequence() {
        return tailSequence;
    }

    public long headSequence() {
        return headSequence;
    }

    // just for testing
    public void setHeadSequence(long sequence) {
        headSequence = sequence;
    }

    public int getCapacity() {
        return capacity;
    }

    public long size() {
        return tailSequence - headSequence + 1;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public boolean shouldWait(long sequence) {
        checkBlockableReadSequence(sequence);

        return sequence == tailSequence + 1;
    }

    public long remainingCapacity() {
        if (isTTLEnabled()) {
            return capacity - size();
        }

        return capacity;
    }

    private boolean isTTLEnabled() {
        return ttlMs != TTL_DISABLED;
    }

    int toIndex(long sequence) {
        return (int) (sequence % ringItems.length);
    }

    void checkReadSequence(long sequence) {
        if (sequence > tailSequence) {
            throw new IllegalArgumentException("sequence:" + sequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }

        if (sequence < headSequence) {
            throw new StaleSequenceException("sequence:" + sequence
                    + " is too small. The current headSequence is:" + headSequence, headSequence);
        }
    }

    public void checkBlockableReadSequence(long readSequence) {
        if (readSequence > tailSequence + 1) {
            throw new IllegalArgumentException("sequence:" + readSequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }

        if (readSequence < headSequence) {
            throw new StaleSequenceException("sequence:" + readSequence
                    + " is too small. The current headSequence is:" + headSequence, headSequence);
        }
    }

    public long add(Data item) {
        return addInternal(item);
    }

    private long addInternal(Data dataItem) {
        tailSequence++;

        if (tailSequence - capacity == headSequence) {
            headSequence++;
        }

        int index = toIndex(tailSequence);

        Object item = dataItem;
        if (inMemoryFormat == OBJECT) {
            item = serializationService.toObject(dataItem);
        }

        // first we write the dataItem in the ring.
        ringItems[index] = item;

        // and then we optionally write the expiration.
        if (isTTLEnabled()) {
            ringExpirationMs[index] = currentTimeMillis() + ttlMs;
        }

        return tailSequence;
    }

    public long addAll(Data[] items) {
        long result = -1;
        for (Data item : items) {
            result = addInternal(item);
        }
        return result;
    }

    public Data read(long sequence) {
        checkReadSequence(sequence);

        int index = toIndex(sequence);
        Object item = ringItems[index];
        return serializationService.toData(item);
    }

    /**
     * @param beginSequence the sequence of the first item to read.
     * @param result        the List where the result are stored in.
     * @return returns the sequenceId of the next item to read. This is needed if not all required items are found.
     */
    public long readMany(long beginSequence, ReadResultSetImpl result) {
        checkReadSequence(beginSequence);

        long seq = beginSequence;
        while (seq <= tailSequence) {
            int index = toIndex(seq);
            Object item = ringItems[index];

            result.addItem(item);

            seq++;

            if (result.isMaxSizeReached()) {
                // we have found all items we are looking for. We are done.
                break;
            }
        }

        return seq;
    }

    /**
     * Cleans up the ringbuffer by deleting all expired items.
     */
    public void cleanup() {
        if (!isTTLEnabled() || headSequence > tailSequence) {
            return;
        }

        long now = currentTimeMillis();
        while (headSequence <= tailSequence) {
            int index = toIndex(headSequence);

            if (ringExpirationMs[index] > now) {
                return;
            }

            // we null the slot and allow the gc to take care of the object.
            // if we don't clean it, we'll have a potential memory leak.
            ringItems[index] = null;

            // we don't need to 0 the ringExpirationMs slot since it contains a long value.

            // and we move the head to the next item.
            // if nothing remains in the ringbuffer, then the head will be 1 larger than the tail.
            headSequence++;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(tailSequence);
        out.writeLong(headSequence);
        out.writeInt(capacity);
        out.writeLong(ttlMs);
        out.writeInt(inMemoryFormat.ordinal());

        boolean ttlEnabled = isTTLEnabled();

        long now = System.currentTimeMillis();

        // we only write the actual content of the ringbuffer. So we don't write empty slots.
        for (long seq = headSequence; seq <= tailSequence; seq++) {
            int index = toIndex(seq);

            if (inMemoryFormat == BINARY) {
                out.writeData((Data) ringItems[index]);
            } else {
                out.writeObject(ringItems[index]);
            }

            // we write the time difference compared to now. Because the clock on the receiving side
            // can be totally different then our time. If there would be a ttl of 10 seconds, than
            // the expiration time would be 10 seconds after the insertion time. But the if ringbuffer is
            // migrated to a machine with a time 1 hour earlier, the ttl would effectively become
            // 1 hours and 10 seconds.
            if (ttlEnabled) {
                long deltaMs = ringExpirationMs[index] - now;
                out.writeLong(deltaMs);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        tailSequence = in.readLong();
        headSequence = in.readLong();
        capacity = in.readInt();
        ttlMs = in.readLong();
        inMemoryFormat = values()[in.readInt()];

        ringItems = new Object[capacity];

        boolean ttlEnabled = isTTLEnabled();
        if (ttlEnabled) {
            ringExpirationMs = new long[capacity];
        }

        long now = System.currentTimeMillis();
        for (long seq = headSequence; seq <= tailSequence; seq++) {
            int index = toIndex(seq);

            if (inMemoryFormat == BINARY) {
                ringItems[index] = in.readData();
            } else {
                ringItems[index] = in.readObject();
            }

            if (ttlEnabled) {
                long delta = in.readLong();
                ringExpirationMs[index] = delta + now;
            }
        }
    }
}

