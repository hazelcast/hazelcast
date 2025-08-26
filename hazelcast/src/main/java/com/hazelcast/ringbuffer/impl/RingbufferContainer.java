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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.internal.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.config.InMemoryFormat.values;
import static com.hazelcast.internal.namespace.NamespaceUtil.callWithNamespace;
import static com.hazelcast.internal.namespace.NamespaceUtil.runWithNamespace;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The RingbufferContainer is responsible for the functionalities supporting
 * the underlying ringbuffer structure containing the data.
 * This includes creating the actual ringbuffer, handling optional ringbuffer
 * storage, keeping the wait/notify key for blocking operations, ringbuffer
 * item expiration and other things not related to the ringbuffer data
 * structure.
 *
 * @param <T> the type of items in the ringbuffer container
 * @param <E> the type of items in the ringbuffer
 */
@SuppressWarnings("checkstyle:methodcount")
public class RingbufferContainer<T, E> implements IdentifiedDataSerializable, Notifier, Versioned {

    private static final long TTL_DISABLED = 0;

    private ObjectNamespace objectNamespace;

    // a cached version of the wait notify key needed to wait for a change if the ringbuffer is empty
    private RingbufferWaitNotifyKey emptyRingWaitNotifyKey;
    /**
     * The expirationPolicy contains the expiration policy of the items. If a
     * time to live is set, the policy is created, otherwise it is {@code null}
     * to save space.
     */
    private RingbufferExpirationPolicy expirationPolicy;
    private InMemoryFormat inMemoryFormat;
    private RingbufferConfig config;
    private RingbufferStoreWrapper store;
    private SerializationService serializationService;
    private String userCodeNamespace;

    /**
     * The ringbuffer containing the items. The type of contained items depends
     * on the {@link #inMemoryFormat} :
     * <ul>
     * <li>{@link InMemoryFormat#OBJECT} - the type is the same as the type
     * {@link T}</li>
     * <li>{@link InMemoryFormat#BINARY} or {@link InMemoryFormat#NATIVE} -
     * the type is {@link Data}</li>
     * </ul>
     */
    private Ringbuffer<E> ringbuffer;

    /**
     * For purposes of {@link IdentifiedDataSerializable} instance creation.
     * For any other purpose, use other constructors in this class.
     */
    public RingbufferContainer() {
    }

    /**
     * Constructs the ring buffer container with only the name and the key for
     * blocking operations. This does not fully prepare the container for usage.
     * The caller must invoke the
     * {@link #init(RingbufferConfig, NodeEngine)}
     * method to complete the initialization before usage.
     *
     * @param objectNamespace   the {@link ObjectNamespace} of the ringbuffer container
     * @param userCodeNamespace the user code namespace of the ringbuffer content
     */
    public RingbufferContainer(ObjectNamespace objectNamespace, int partitionId, String userCodeNamespace) {
        this.objectNamespace = objectNamespace;
        this.userCodeNamespace = userCodeNamespace;
        this.emptyRingWaitNotifyKey = new RingbufferWaitNotifyKey(objectNamespace, partitionId);
    }

    /**
     * Constructs a fully initialized ringbuffer that can be used immediately.
     * References to other services, the ringbuffer store, expiration policy
     * and the config are all set.
     *
     * @param namespace  the namespace of the ring buffer container
     * @param config     the configuration of the ring buffer
     * @param nodeEngine the NodeEngine
     */
    public RingbufferContainer(ObjectNamespace namespace,
                               RingbufferConfig config,
                               NodeEngine nodeEngine,
                               int partitionId) {
        this(namespace, partitionId, config.getUserCodeNamespace());

        this.inMemoryFormat = config.getInMemoryFormat();
        this.ringbuffer = new ArrayRingbuffer<>(config.getCapacity());

        final long ttlMs = SECONDS.toMillis(config.getTimeToLiveSeconds());
        if (ttlMs != TTL_DISABLED) {
            this.expirationPolicy = new RingbufferExpirationPolicy(ringbuffer.getCapacity(), ttlMs);
        }
        init(config, nodeEngine);
    }

    /**
     * Initializes the ring buffer with references to other services, the
     * ringbuffer store and the config. This is because on a replication
     * operation the container is only partially constructed. The init method
     * finishes the configuration of the ring buffer container for further
     * usage.
     *
     * @param config     the configuration of the ring buffer
     * @param nodeEngine the NodeEngine
     */
    public void init(RingbufferConfig config, NodeEngine nodeEngine) {
        this.config = config;
        this.userCodeNamespace = config.getUserCodeNamespace();
        this.serializationService = nodeEngine.getSerializationService();
        initRingbufferStore(NamespaceUtil.getClassLoaderForNamespace(nodeEngine, config.getUserCodeNamespace()), nodeEngine);
    }

    private void initRingbufferStore(ClassLoader classLoader, NodeEngine nodeEngine) {
        this.store = RingbufferStoreWrapper.create(objectNamespace,
                config.getRingbufferStoreConfig(),
                config.getInMemoryFormat(),
                serializationService,
                classLoader,
                nodeEngine,
                config.getUserCodeNamespace());
        if (store.isEnabled()) {
            try {
                final long storeSequence = store.getLargestSequence();
                if (tailSequence() < storeSequence) {
                    ringbuffer.setTailSequence(storeSequence);
                    ringbuffer.setHeadSequence(storeSequence + 1);
                }
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
    }

    public RingbufferStoreWrapper getStore() {
        return store;
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

    public String getUserCodeNamespace() {
        return userCodeNamespace;
    }

    /**
     * Sets the head sequence. The head sequence cannot be larger than
     * {@code tailSequence() + 1}
     *
     * @param sequence the new head sequence
     * @throws IllegalArgumentException if the target sequence is greater than {@code tailSequence() + 1}
     * @see #tailSequence()
     */
    public void setHeadSequence(long sequence) {
        ringbuffer.setHeadSequence(sequence);
    }

    /**
     * Sets the tail sequence. The tail sequence cannot be less than
     * {@code headSequence() - 1}.
     *
     * @param sequence the new tail sequence
     * @throws IllegalArgumentException if the target sequence is less than
     *                                  {@code headSequence() - 1}
     * @see #headSequence()
     */
    public void setTailSequence(long sequence) {
        ringbuffer.setTailSequence(sequence);
    }

    /**
     * Returns the capacity of this ringbuffer.
     *
     * @return the capacity
     */
    public long getCapacity() {
        return ringbuffer.getCapacity();
    }

    public long size() {
        return ringbuffer.size();
    }

    // not used in the codebase, here just for API completion
    public boolean isEmpty() {
        return ringbuffer.isEmpty();
    }

    /**
     * Returns if the sequence is one after the sequence of the newest item in
     * the ring buffer. In this case, the caller needs to wait for the item to
     * be available. This method will throw an exception if the requested
     * instance is larger than the sequence of the next item to be added (e.g.
     * the second or third next item to added).
     * If the ring buffer store is configured and enabled, this method will
     * allow sequence values older than the head sequence.
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
     * Returns the remaining capacity of the ringbuffer. If TTL is enabled,
     * then the returned capacity is equal to the total capacity of the
     * ringbuffer minus the number of used slots in the ringbuffer which have
     * not yet been marked as expired and cleaned up. Keep in mind that some
     * slots could have expired items that have not yet been cleaned up and
     * that the returned value could be stale as soon as it is returned.
     * <p>
     * If TTL is disabled, the remaining capacity is equal to the total
     * ringbuffer capacity.
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
     * Adds one item to the ring buffer. Sets the expiration time if TTL is
     * configured and also attempts to store the item in the data store if one
     * is configured. The provided {@code item} can be {@link Data} or the
     * deserialized object.
     * The provided item will be transformed to the configured ringbuffer
     * {@link InMemoryFormat} if necessary.
     *
     * @param item item to be stored in the ring buffer and data store, can be
     *             {@link Data} or a deserialized object
     * @return the sequence ID of the item stored in the ring buffer
     * @throws HazelcastException              if there was any exception thrown by the data store
     * @throws HazelcastSerializationException if the ring buffer is configured to keep items in object format and the
     *                                         item could not be deserialized
     */
    public long add(T item) {
        final long nextSequence = ringbuffer.peekNextTailSequence();
        if (store.isEnabled()) {
            try {
                store.store(nextSequence, convertToData(item));
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }

        final long storedSequence = addInternal(item);
        if (storedSequence != nextSequence) {
            throw new IllegalStateException("Sequence we stored the item with and Ringbuffer sequence differs. Was the "
                    + "Ringbuffer mutated from multiple threads?");
        }

        return storedSequence;
    }

    /**
     * Adds all items to the ringbuffer. Sets the expiration time if TTL is
     * configured and also attempts to store the items in the data store if one
     * is configured.
     *
     * @param items items to be stored in the ring buffer and data store
     * @return the sequence ID of the last item stored in the ring buffer
     * @throws HazelcastException              if there was any exception thrown by the data store
     * @throws HazelcastSerializationException if the ring buffer is configured to keep items
     *                                         in object format and the item could not be
     *                                         deserialized
     */
    public long addAll(T[] items) {
        long firstSequence = ringbuffer.peekNextTailSequence();
        long lastSequence = ringbuffer.peekNextTailSequence();

        if (store.isEnabled() && items.length != 0) {
            try {
                store.storeAll(firstSequence, convertToData(items));
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }

        for (T item : items) {
            lastSequence = addInternal(item);
        }
        return lastSequence;
    }

    /**
     * Sets the item at the given sequence ID and updates the expiration time
     * if TTL is configured.
     * Unlike other methods for adding items into the ring buffer, does not
     * attempt to store the item in the data store. This method expands the
     * ring buffer tail and head sequence to accommodate for the sequence.
     * This means that it will move the head or tail sequence to the target
     * sequence if the target sequence is less than the head sequence or
     * greater than the tail sequence.
     *
     * @param sequenceId the sequence ID under which the item is stored
     * @param item       item to be stored in the ring buffer and data store
     * @throws HazelcastSerializationException if the ring buffer is configured to keep items in object format and the
     *                                         item could not be deserialized
     */
    public void set(long sequenceId, T item) {
        final E rbItem = convertToRingbufferFormat(item);

        // first we write the dataItem in the ring.
        ringbuffer.set(sequenceId, rbItem);

        if (sequenceId > tailSequence()) {
            ringbuffer.setTailSequence(sequenceId);

            if (ringbuffer.size() > ringbuffer.getCapacity()) {
                ringbuffer.setHeadSequence(ringbuffer.tailSequence() - ringbuffer.getCapacity() + 1);
            }
        }
        if (sequenceId < headSequence()) {
            ringbuffer.setHeadSequence(sequenceId);
        }

        // and then we optionally write the expiration.
        if (expirationPolicy != null) {
            expirationPolicy.setExpirationAt(sequenceId);
        }
    }

    /**
     * Reads one item from the ring buffer and returns the serialized format.
     * If the item is not available, it will try and load it from the ringbuffer store.
     * If the stored format is already serialized, there is no serialization.
     *
     * @param sequence The sequence of the item to be read
     * @return The item read
     * @throws StaleSequenceException if the sequence is :
     *                                1. larger than the tailSequence or
     *                                2. smaller than the headSequence and the data store is disabled
     */
    public Data readAsData(long sequence) {
        checkReadSequence(sequence);
        return callWithNamespace(userCodeNamespace, () -> {
            Object rbItem = readOrLoadItem(sequence);
            return serializationService.toData(rbItem);
        });
    }

    /**
     * Reads multiple items from the ring buffer and adds them to {@code result}
     * in the stored format. If an item is not available, it will try and
     * load it from the ringbuffer store.
     *
     * @param beginSequence the sequence of the first item to read.
     * @param result        the List where the result are stored in.
     * @return returns the sequenceId of the next item to read. This is needed if not all required items are found.
     * @throws StaleSequenceException if the sequence is :
     *                                1. larger than the tailSequence or
     *                                2. smaller than the headSequence and the data store is disabled
     */
    public long readMany(long beginSequence, ReadResultSetImpl result) {
        checkReadSequence(beginSequence);

        return callWithNamespace(userCodeNamespace, () -> {
            long seq = beginSequence;
            while (seq <= ringbuffer.tailSequence()) {
                result.addItem(seq, readOrLoadItem(seq));
                seq++;
                if (result.isMaxSizeReached()) {
                    // we have found all items we are looking for. We are done.
                    break;
                }
            }
            return seq;
        });
    }

    public void cleanup() {
        if (expirationPolicy != null) {
            expirationPolicy.cleanup(ringbuffer);
        }
    }

    public boolean isStaleSequence(long sequence) {
        return sequence < headSequence() && !store.isEnabled();
    }

    public boolean isTooLargeSequence(long sequence) {
        return sequence > tailSequence() + 1;
    }

    /**
     * Checks if the provided {@code sequence} can be read immediately. If
     * not, it will return the current oldest or newest immediately readable
     * sequence. This method can be used for a loss-tolerant reader when
     * trying to avoid a {@link com.hazelcast.ringbuffer.StaleSequenceException}.
     *
     * @param readSequence the sequence wanting to be read
     * @return the bounded sequence
     */
    public long clampReadSequenceToBounds(long readSequence) {
        // fast-forward if late and no store is configured
        final long headSequence = headSequence();
        if (readSequence < headSequence && !store.isEnabled()) {
            return headSequence;
        }

        // jump back if too far in future
        final long tailSequence = tailSequence();
        if (readSequence > tailSequence + 1) {
            return tailSequence + 1;
        }

        return readSequence;
    }

    /**
     * Check if the sequence is of an item that can be read immediately
     * or is the sequence of the next item to be added into the ringbuffer.
     * Since this method allows the sequence to be one larger than
     * the {@link #tailSequence()}, the caller can use this method
     * to check the sequence before performing a possibly blocking read.
     * Also, the requested sequence can be smaller than the head sequence
     * if the data store is enabled.
     *
     * @param readSequence the sequence wanting to be read
     * @throws StaleSequenceException   if the requested sequence is smaller than the head sequence and the data store is
     *                                  not enabled
     * @throws IllegalArgumentException if the requested sequence is greater than the tail sequence + 1 or
     */
    public void checkBlockableReadSequence(long readSequence) {
        if (isTooLargeSequence(readSequence)) {
            throw new IllegalArgumentException("sequence:" + readSequence
                    + " is too large. The current tailSequence is:" + tailSequence());
        }
        if (isStaleSequence(readSequence)) {
            throw new StaleSequenceException("sequence:" + readSequence
                    + " is too small and data store is disabled. "
                    + "The current headSequence is:" + headSequence()
                    + " tailSequence is:" + tailSequence(), headSequence());
        }
    }

    /**
     * Check if the sequence can be read from the ring buffer.
     *
     * @param sequence the sequence wanting to be read
     * @throws StaleSequenceException   if the requested sequence is smaller than the head sequence and the data
     *                                  store is not enabled
     * @throws IllegalArgumentException if the requested sequence is greater than the tail sequence
     */
    private void checkReadSequence(long sequence) {
        final long tailSequence = ringbuffer.tailSequence();
        if (sequence > tailSequence) {
            throw new IllegalArgumentException("sequence:" + sequence
                    + " is too large. The current tailSequence is:" + tailSequence);
        }
        if (isStaleSequence(sequence)) {
            throw new StaleSequenceException("sequence:" + sequence
                    + " is too small and data store is disabled."
                    + " The current headSequence is:" + headSequence()
                    + " tailSequence is:" + tailSequence, headSequence());
        }
    }

    /**
     * Reads the item at the specified sequence or loads it from the ringbuffer
     * store if one is enabled. The type of the returned object is equal to the
     * ringbuffer format.
     */
    private Object readOrLoadItem(long sequence) {
        Object item;
        if (sequence < ringbuffer.headSequence() && store.isEnabled()) {
            item = store.load(sequence);
        } else {
            item = ringbuffer.read(sequence);
        }
        return item;
    }

    private long addInternal(T item) {
        final E rbItem = convertToRingbufferFormat(item);

        // first we write the dataItem in the ring.
        final long tailSequence = ringbuffer.add(rbItem);

        // and then we optionally write the expiration.
        if (expirationPolicy != null) {
            expirationPolicy.setExpirationAt(tailSequence);
        }
        return tailSequence;
    }

    /**
     * Converts the {@code item} into the ringbuffer {@link InMemoryFormat} or
     * keeps it unchanged if the supplied argument is already in the ringbuffer
     * format.
     *
     * @param item the item
     * @return the binary or deserialized format, depending on the {@link RingbufferContainer#inMemoryFormat}
     * @throws HazelcastSerializationException if the ring buffer is configured to keep items
     *                                         in object format and the item could not be deserialized
     */
    private E convertToRingbufferFormat(Object item) {
        return callWithNamespace(userCodeNamespace, () -> inMemoryFormat == OBJECT
                ? (E) serializationService.toObject(item)
                : (E) serializationService.toData(item));
    }

    /**
     * Convert the supplied argument into serialized format.
     *
     * @throws HazelcastSerializationException when serialization fails.
     */
    private Data convertToData(Object item) {
        return serializationService.toData(item);
    }

    /**
     * Convert the supplied argument into serialized format.
     *
     * @throws HazelcastSerializationException when serialization fails.
     */
    private Data[] convertToData(T[] items) {
        if (items == null || items.length == 0) {
            return new Data[0];
        }
        if (items[0] instanceof Data) {
            return (Data[]) items;
        }
        final Data[] ret = new Data[items.length];
        for (int i = 0; i < items.length; i++) {
            ret[i] = convertToData(items[i]);
        }
        return ret;
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
                IOUtil.writeData(out, (Data) ringbuffer.read(seq));
            } else {
                out.writeObject(ringbuffer.read(seq));
            }

            // we write the time difference compared to now. Because the clock on the receiving side
            // can be totally different then our time. If there would be a ttl of 10 seconds, than
            // the expiration time would be 10 seconds after the insertion time. But if ringbuffer is
            // migrated to a machine with a time 1 hour earlier, the ttl would effectively become
            // 1 hour and 10 seconds.
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
        runWithNamespace(userCodeNamespace, () -> {
            for (long seq = headSequence; seq <= tailSequence; seq++) {
                final long seqFinal = seq;
                if (inMemoryFormat == BINARY) {
                    runWithNamespace(userCodeNamespace, () -> ringbuffer.set(seqFinal, (E) IOUtil.readData(in)));
                } else {
                    runWithNamespace(userCodeNamespace, () -> ringbuffer.set(seqFinal, in.readObject()));
                }

                if (ttlEnabled) {
                    long delta = in.readLong();
                    expirationPolicy.setExpirationAt(seq, delta + now);
                }
            }
        });
    }

    /**
     * Returns the ringbuffer containing the actual items.
     */
    public Ringbuffer<E> getRingbuffer() {
        return ringbuffer;
    }

    RingbufferExpirationPolicy getExpirationPolicy() {
        return expirationPolicy;
    }

    /**
     * @deprecated due to ambiguity between User Code Namespaces (used in
     * most data structures) and ObjectNamespaces not used in most data structures
     *
     * @return This container's {@link ObjectNamespace}
     */
    @Deprecated(since = "5.4")
    public ObjectNamespace getNamespace() {
        return objectNamespace;
    }

    public ObjectNamespace getObjectNamespace() {
        return objectNamespace;
    }

    @Override
    public int getFactoryId() {
        return RingbufferDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RingbufferDataSerializerHook.RINGBUFFER_CONTAINER;
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return emptyRingWaitNotifyKey;
    }

    /**
     * Clears the data in the ringbuffer.
     */
    public void clear() {
        ringbuffer.clear();
        if (expirationPolicy != null) {
            expirationPolicy.clear();
        }
    }
}
