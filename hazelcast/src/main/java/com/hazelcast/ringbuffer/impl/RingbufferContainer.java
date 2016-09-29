/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.InMemoryFormat.values;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * The RingbufferContainer is responsible for creating the actual ring buffer, handling optional ring buffer storage,
 * keeping the wait/notify key for blocking operations, ring buffer item expiration and other things not related to the
 * actual ring buffer data structure.
 * <p>
 * The ringExpiration contains the expiration time of an item. If a time to live is set, the ringExpiration array is created,
 * therwise it is null to save space.
 */
public class RingbufferContainer implements DataSerializable {
    private static final long TTL_DISABLED = 0;

    String name;

    // a cached version of the wait notify key needed to wait for a change if the ringbuffer is empty
    private final RingbufferWaitNotifyKey emptyRingWaitNotifyKey;
    private RingbufferExpirationPolicy expirationPolicy;
    private InMemoryFormat inMemoryFormat;
    private RingbufferConfig config;
    private RingbufferStoreWrapper store;
    private SerializationService serializationService;

    private Ringbuffer ringbuffer;

    /**
     * Constructs the ring buffer container with only the name and the key for blocking operations. This does not fully
     * prepare the container for usage. The caller must invoke the
     * {@link #init(String, RingbufferConfig, SerializationService, ClassLoader)} method to complete the initialization before
     * usage.
     *
     * @param name the name of the ring buffer container
     */
    public RingbufferContainer(String name) {
        this.name = name;
        this.emptyRingWaitNotifyKey = new RingbufferWaitNotifyKey(name, "empty");
    }

    public RingbufferContainer(String name, RingbufferConfig config,
                               SerializationService serializationService,
                               ClassLoader configClassLoader) {
        this(name);

        this.inMemoryFormat = config.getInMemoryFormat();
        this.ringbuffer = new ArrayRingbuffer(config.getCapacity());

        final long ttlMs = SECONDS.toMillis(config.getTimeToLiveSeconds());
        if (ttlMs != TTL_DISABLED) {
            this.expirationPolicy = new RingbufferExpirationPolicy(ringbuffer.getCapacity(), ttlMs);
        }
        init(name, config, serializationService, configClassLoader);
    }

    /**
     * Initializes the ring buffer with references to other services, the ring buffer store and the config. This is because
     * on a replication operation the container is only partially constructed. The init method finishes the configuration
     * of the ring buffer container for further usage.
     *
     * @param name                 the name of the ring buffer
     * @param config               the configuration of the ring buffer
     * @param serializationService the serialization service
     * @param configClassLoader    the class loader for which the ring buffer store classes will be loaded
     */
    public void init(String name, RingbufferConfig config,
                     SerializationService serializationService,
                     ClassLoader configClassLoader) {
        this.config = config;
        this.serializationService = serializationService;
        initRingbufferStore(name, config, serializationService, configClassLoader);
    }

    private void initRingbufferStore(String name, RingbufferConfig config,
                                     SerializationService serializationService, ClassLoader configClassLoader) {
        this.store = RingbufferStoreWrapper.create(name,
                config.getRingbufferStoreConfig(),
                config.getInMemoryFormat(),
                serializationService,
                configClassLoader);
        if (store.isEnabled()) {
            try {
                final long storeSequence = store.getLargestSequence();
                ringbuffer.setTailSequence(storeSequence);
                ringbuffer.setHeadSequence(storeSequence + 1);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
    }

    /**
     * Gets the wait/notify key for the blocking operations of reading from the ring buffer.
     *
     * @return the wait/notify key
     */
    public RingbufferWaitNotifyKey getRingEmptyWaitNotifyKey() {
        return emptyRingWaitNotifyKey;
    }

    public RingbufferConfig getConfig() {
        return config;
    }

    public long tailSequence() {
        return ringbuffer.tailSequence();
    }

    public long headSequence() {
        return ringbuffer.headSequence();
    }

    // just for testing
    public void setHeadSequence(long sequence) {
        ringbuffer.setHeadSequence(sequence);
    }

    public long getCapacity() {
        return ringbuffer.getCapacity();
    }

    public long size() {
        return ringbuffer.size();
    }

    public boolean isEmpty() {
        return ringbuffer.isEmpty();
    }

    /**
     * Returns if the sequence is one after the sequence of the newest item in the ring buffer. In this case, the caller
     * needs to wait for the item to be available. This method will throw an exception if the requested instance is larger than
     * the sequence of the next item to be added (e.g. the second or third next item to added). If the ring buffer store is
     * configured and enabled, this method will allow sequence values older than the head sequence.
     *
     * @param sequence the requested sequence
     * @return should the caller wait for
     * @throws StaleSequenceException if the requested sequence is
     *                                1. greater than the tail sequence + 1 or
     *                                2. smaller than the head sequence and the data store is not enabled
     */
    public boolean shouldWait(long sequence) {
        checkBlockableReadSequence(sequence);
        return sequence == ringbuffer.tailSequence() + 1;
    }

    /**
     * Returns the remaining capacity of the ring buffer. If TTL is enabled, then the returned capacity is equal to the
     * total capacity of the ringbuffer minus the number of used slots in the ringbuffer which have not yet been marked as
     * expired and cleaned up. Keep in mind that some slots could have expired items that have not yet been cleaned up and
     * that the returned value could be stale as soon as it is returned.
     * <p>
     * If TTL is disabled, the remaining capacity is equal to the total ringbuffer capacity.
     *
     * @return the remaining capacity of the ring buffer
     */
    public long remainingCapacity() {
        if (expirationPolicy != null) {
            return ringbuffer.getCapacity() - size();
        }

        return ringbuffer.getCapacity();
    }


    /**
     * Adds one item to the ring buffer. Also attempts to store the item in the data store if one is configured.
     *
     * @param item items to be stored in the ring buffer and data store
     * @return the sequence id of the item stored in the ring buffer
     * @throws HazelcastException                                              if there was any exception thrown by the data store
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException if the ring buffer is configured to keep items
     *                                                                         in object format and the item could not be
     *                                                                         deserialized
     */
    public long add(Data item) {
        final long sequence = addInternal(item);
        if (store.isEnabled()) {
            try {
                store.store(sequence, item);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        return sequence;
    }

    /**
     * Adds all items to the ring buffer. Also attempts to store the items in the data store if one is configured.
     *
     * @param items items to be stored in the ring buffer and data store
     * @return the sequence id of the last item stored in the ring buffer
     * @throws HazelcastException                                              if there was any exception thrown by the data store
     * @throws com.hazelcast.nio.serialization.HazelcastSerializationException if the ring buffer is configured to keep items
     *                                                                         in object format and the item could not be
     *                                                                         deserialized
     */
    public long addAll(Data[] items) {
        long firstSequence = -1;
        long lastSequence = -1;

        for (int i = 0; i < items.length; i++) {
            lastSequence = addInternal(items[i]);
            if (i == 0) {
                firstSequence = lastSequence;
            }
        }
        if (store.isEnabled() && items.length != 0) {
            try {
                store.storeAll(firstSequence, items);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        return firstSequence;
    }

    /**
     * Reads one item from the ring buffer.
     *
     * @param sequence The sequence of the item to be read
     * @return The item read
     * @throws StaleSequenceException if the sequence is :
     *                                1. larger than the tailSequence or
     *                                2. smaller than the headSequence and the data store is disabled
     */
    public Data read(long sequence) {
        checkReadSequence(sequence);
        Object item = readOrLoadItem(sequence);
        return serializationService.toData(item);
    }

    /**
     * @param beginSequence the sequence of the first item to read.
     * @param result        the List where the result are stored in.
     * @return returns the sequenceId of the next item to read. This is needed if not all required items are found.
     * @throws StaleSequenceException if the sequence is :
     *                                1. larger than the tailSequence or
     *                                2. smaller than the headSequence and the data store is disabled
     */
    public long readMany(long beginSequence, ReadResultSetImpl result) {
        checkReadSequence(beginSequence);

        long seq = beginSequence;
        while (seq <= ringbuffer.tailSequence()) {
            result.addItem(readOrLoadItem(seq));
            seq++;
            if (result.isMaxSizeReached()) {
                // we have found all items we are looking for. We are done.
                break;
            }
        }
        return seq;
    }

    @SuppressWarnings("unchecked")
    public void cleanup() {
        if (expirationPolicy != null) {
            expirationPolicy.cleanup(ringbuffer);
        }
    }

    /**
     * Check if the sequence can be read from the ring buffer or if the sequence is of the next item to be added into the
     * ringbuffer. This method also allows the sequence to be one greater than the tail sequence, giving the oportunity to
     * block until the item is added into the ring buffer. Also, the requested sequence can be smaller than the
     * head sequence if the data store is enabled.
     *
     * @param readSequence the sequence wanting to be read
     * @throws StaleSequenceException if the requested sequence is
     *                                1. greater than the tail sequence + 1 or
     *                                2. smaller than the head sequence and the data store is not enabled
     */
    public void checkBlockableReadSequence(long readSequence) {
        final long tailSequence = ringbuffer.tailSequence();

        if (readSequence > tailSequence + 1) {
            throw new IllegalArgumentException("sequence:" + readSequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }

        final long headSequence = ringbuffer.headSequence();
        if (readSequence < headSequence && !store.isEnabled()) {
            throw new StaleSequenceException("sequence:" + readSequence
                    + " is too small and data store is disabled. "
                    + "The current headSequence is:" + headSequence
                    + " tailSequence is:" + tailSequence, headSequence);
        }
    }


    /**
     * Check if the sequence can be read from the ring buffer.
     *
     * @param sequence the sequence wanting to be read
     * @throws StaleSequenceException if the requested sequence is
     *                                1. greater than the tail sequence or
     *                                2. smaller than the head sequence and the data store is not enabled
     */
    private void checkReadSequence(long sequence) {
        final long tailSequence = ringbuffer.tailSequence();
        if (sequence > tailSequence) {
            throw new IllegalArgumentException("sequence:" + sequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }

        final long headSequence = ringbuffer.headSequence();
        if (sequence < headSequence && !store.isEnabled()) {
            throw new StaleSequenceException("sequence:" + sequence
                    + " is too small and data store is disabled."
                    + " The current headSequence is:" + headSequence
                    + " tailSequence is:" + tailSequence, headSequence);
        }
    }

    private Object readOrLoadItem(long sequence) {
        Object item;
        if (sequence < ringbuffer.headSequence() && store.isEnabled()) {
            item = store.load(sequence);
        } else {
            item = ringbuffer.read(sequence);
        }
        return item;
    }

    @SuppressWarnings("unchecked")
    private long addInternal(Data dataItem) {
        Object item = dataItem;
        if (inMemoryFormat == OBJECT) {
            item = serializationService.toObject(dataItem);
        }

        // first we write the dataItem in the ring.
        final long tailSequence = ringbuffer.add(item);

        // and then we optionally write the expiration.
        if (expirationPolicy != null) {
            expirationPolicy.setExpirationAt(tailSequence);
        }
        return tailSequence;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        boolean ttlEnabled = expirationPolicy != null;
        out.writeLong(ringbuffer.tailSequence());
        out.writeLong(ringbuffer.headSequence());
        out.writeInt((int) ringbuffer.getCapacity());
        out.writeLong(ttlEnabled ? expirationPolicy.getTtlMs() : 0);
        out.writeInt(inMemoryFormat.ordinal());

        long now = System.currentTimeMillis();

        // we only write the actual content of the ringbuffer. So we don't write empty slots.
        for (long seq = ringbuffer.headSequence(); seq <= ringbuffer.tailSequence(); seq++) {
            if (inMemoryFormat == BINARY) {
                out.writeData((Data) ringbuffer.read(seq));
            } else {
                out.writeObject(ringbuffer.read(seq));
            }

            // we write the time difference compared to now. Because the clock on the receiving side
            // can be totally different then our time. If there would be a ttl of 10 seconds, than
            // the expiration time would be 10 seconds after the insertion time. But the if ringbuffer is
            // migrated to a machine with a time 1 hour earlier, the ttl would effectively become
            // 1 hours and 10 seconds.
            if (ttlEnabled) {
                long deltaMs = expirationPolicy.getExpirationAt(seq) - now;
                out.writeLong(deltaMs);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readData(ObjectDataInput in) throws IOException {
        final long tailSequence = in.readLong();
        final long headSequence = in.readLong();
        final int capacity = in.readInt();
        final long ttlMs = in.readLong();
        inMemoryFormat = values()[in.readInt()];

        ringbuffer = new ArrayRingbuffer(capacity);
        ringbuffer.setTailSequence(tailSequence);
        ringbuffer.setHeadSequence(headSequence);

        boolean ttlEnabled = ttlMs != TTL_DISABLED;
        if (ttlEnabled) {
            this.expirationPolicy = new RingbufferExpirationPolicy(capacity, ttlMs);
        }

        long now = System.currentTimeMillis();
        for (long seq = headSequence; seq <= tailSequence; seq++) {
            if (inMemoryFormat == BINARY) {
                ringbuffer.set(seq, in.readData());
            } else {
                ringbuffer.set(seq, in.readObject());
            }

            if (ttlEnabled) {
                long delta = in.readLong();
                expirationPolicy.setExpirationAt(seq, delta + now);
            }
        }
    }

    Ringbuffer getRingbuffer() {
        return ringbuffer;
    }

    RingbufferExpirationPolicy getExpirationPolicy() {
        return expirationPolicy;
    }
}

