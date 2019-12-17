/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * The {@code FlakeIdGeneratorConfig} contains the configuration for the member
 * regarding {@link com.hazelcast.core.HazelcastInstance#getFlakeIdGenerator(String)
 * Flake ID Generator}.
 *
 * @since 3.10
 */
public class FlakeIdGeneratorConfig implements IdentifiedDataSerializable, NamedConfig {

    /**
     * Default value for {@link #getPrefetchCount()}.
     */
    public static final int DEFAULT_PREFETCH_COUNT = 100;

    /**
     * Default value for {@link #getPrefetchValidityMillis()}.
     */
    public static final long DEFAULT_PREFETCH_VALIDITY_MILLIS = 600000;

    /**
     * {@code 1514764800000} is the value {@code System.currentTimeMillis()} would return on
     * 1.1.2018 0:00 UTC.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final long DEFAULT_EPOCH_START = 1514764800000L;

    /**
     * Timestamp component bit length, default 41 bits. Default value for {@link #getBitsTimestamp()}.
     */
    public static final int DEFAULT_BITS_TIMESTAMP = 41;
    /**
     * Sequence component bit length, default 6 bits. Default value for {@link #getBitsSequence()}.
     */
    public static final int DEFAULT_BITS_SEQUENCE = 6;
    /**
     * Node ID component bit length, default 16 bits. Default value for {@link #getBitsNodeId()}.
     */
    public static final int DEFAULT_BITS_NODE_ID = 16;
    /**
     * How far to the future is it allowed to go to generate IDs, default 15 seconds.
     * Default value for {@link #getAllowedFutureMillis()}.
     */
    public static final long DEFAULT_ALLOWED_FUTURE_MILLIS = 15000;


    /**
     * Maximum value for prefetch count. The limit is ~10% of the time we allow the IDs to be from the future
     * (see {@link #DEFAULT_ALLOWED_FUTURE_MILLIS}).
     * <p>
     * The reason to limit the prefetch count is that a single call to {@link FlakeIdGenerator#newId()} might
     * be blocked if the future allowance is exceeded: we want to avoid a single call for large batch to block
     * another call for small batch.
     */
    public static final int MAXIMUM_PREFETCH_COUNT = 100000;

    private String name;
    private int prefetchCount = DEFAULT_PREFETCH_COUNT;
    private long prefetchValidityMillis = DEFAULT_PREFETCH_VALIDITY_MILLIS;
    private long epochStart = DEFAULT_EPOCH_START;
    private long idOffset;
    private long nodeIdOffset;
    private int bitsTimestamp = DEFAULT_BITS_TIMESTAMP;
    private int bitsSequence = DEFAULT_BITS_SEQUENCE;
    private int bitsNodeId = DEFAULT_BITS_NODE_ID;
    private long allowedFutureMillis = DEFAULT_ALLOWED_FUTURE_MILLIS;
    private boolean statisticsEnabled = true;

    // for deserialization
    public FlakeIdGeneratorConfig() {
    }

    public FlakeIdGeneratorConfig(String name) {
        this.name = name;
    }

    /**
     * Copy-constructor
     */
    public FlakeIdGeneratorConfig(FlakeIdGeneratorConfig other) {
        this.name = other.name;
        this.prefetchCount = other.prefetchCount;
        this.prefetchValidityMillis = other.prefetchValidityMillis;
        this.epochStart = other.epochStart;
        this.idOffset = other.idOffset;
        this.nodeIdOffset = other.nodeIdOffset;
        this.bitsTimestamp = other.bitsTimestamp;
        this.bitsSequence = other.bitsSequence;
        this.bitsNodeId = other.bitsNodeId;
        this.allowedFutureMillis = other.allowedFutureMillis;
        this.statisticsEnabled = other.statisticsEnabled;
    }

    /**
     * Returns the configuration name. This can be actual object name or pattern.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name or name pattern for this config. Must not be modified after this
     * instance is added to {@link Config}.
     */
    public FlakeIdGeneratorConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @see #setPrefetchCount(int)
     */
    public int getPrefetchCount() {
        return prefetchCount;
    }

    /**
     * Sets how many IDs are pre-fetched on the background when one call to
     * {@link FlakeIdGenerator#newId()} is made. Default is 100.
     * <p>
     * This setting pertains only to {@link FlakeIdGenerator#newId newId} calls made on the member
     * that configured it.
     *
     * @param prefetchCount the desired prefetch count, in the range 1..100,000.
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setPrefetchCount(int prefetchCount) {
        checkTrue(prefetchCount > 0 && prefetchCount <= MAXIMUM_PREFETCH_COUNT,
                "prefetch-count must be 1.." + MAXIMUM_PREFETCH_COUNT + ", not " + prefetchCount);
        this.prefetchCount = prefetchCount;
        return this;
    }

    /**
     * @see #setPrefetchValidityMillis(long)
     */
    public long getPrefetchValidityMillis() {
        return prefetchValidityMillis;
    }

    /**
     * Sets for how long the pre-fetched IDs can be used. If this time elapses, a new batch of IDs will be
     * fetched. Time unit is milliseconds, default is 600,000 (10 minutes).
     * <p>
     * The IDs contain timestamp component, which ensures rough global ordering of IDs. If an ID
     * is assigned to an object that was created much later, it will be much out of order. If you don't care
     * about ordering, set this value to 0.
     * <p>
     * This setting pertains only to {@link FlakeIdGenerator#newId newId} calls made on the member
     * that configured it.
     *
     * @param prefetchValidityMs the desired ID validity or unlimited, if &lt;=0
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setPrefetchValidityMillis(long prefetchValidityMs) {
        this.prefetchValidityMillis = prefetchValidityMs;
        return this;
    }

    /**
     * Sets the offset of timestamp component in milliseconds.
     * <p>
     * This setting pertains only to {@link FlakeIdGenerator#newId newId} calls made on the member
     * that configured it.
     *
     * @param epochStart the desired epoch start
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setEpochStart(long epochStart) {
        this.epochStart = epochStart;
        return this;
    }

    /**
     * @see #setEpochStart(long)
     */
    public long getEpochStart() {
        return epochStart;
    }

    /**
     * @see #setIdOffset(long)
     */
    public long getIdOffset() {
        return idOffset;
    }

    /**
     * Sets the offset that will be added to the returned IDs. Default value is 0. Setting might be useful when
     * migrating from {@code IdGenerator}, default value works for all green-field projects.
     * <p>
     * For example: Largest ID returned from {@code IdGenerator} is 150. {@code FlakeIdGenerator} now
     * returns 100. If you configure {@code idOffset} of 50 and stop using the {@code IdGenerator}, the next
     * ID from {@code FlakeIdGenerator} will be 151 or larger and no duplicate IDs will be generated.
     * In real-life, the IDs are much larger. You also need to add a reserve to the offset because the IDs from
     * {@code FlakeIdGenerator} are only roughly ordered. Recommended reserve is {@code 1<<38},
     * that is 274877906944.
     * <p>
     * Negative values are allowed to increase the lifespan of the generator, however keep in mind that
     * the generated IDs might also be negative.
     *
     * @param idOffset the value added to each generated ID
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setIdOffset(long idOffset) {
        this.idOffset = idOffset;
        return this;
    }

    /**
     * @see #setNodeIdOffset(long)
     */
    public long getNodeIdOffset() {
        return nodeIdOffset;
    }

    /**
     * Sets the offset that will be added to the node ID assigned to cluster member for this generator.
     * Might be useful in A/B deployment scenarios where you have cluster A which you want to upgrade.
     * You create cluster B and for some time both will generate IDs and you want to have them unique.
     * In this case, configure node ID offset for generators on cluster B.
     *
     * @param nodeIdOffset the value added to the node id
     * @return this instance for fluent API
     * @see FlakeIdGenerator for the node id logic
     */
    public FlakeIdGeneratorConfig setNodeIdOffset(long nodeIdOffset) {
        checkNotNegative(nodeIdOffset, "node id offset must be non-negative");
        this.nodeIdOffset = nodeIdOffset;
        return this;
    }

    /**
     * @see #setBitsTimestamp(int)
     */
    public int getBitsTimestamp() {
        return bitsTimestamp;
    }

    /**
     * Sets the bit length of timestamp component.
     *
     * @param bitsTimestamp timestamp component bit length
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setBitsTimestamp(int bitsTimestamp) {
        checkNotNegative(bitsTimestamp, "timestamp bit length must be non-negative");
        this.bitsTimestamp = bitsTimestamp;
        return this;
    }

    /**
     * @see #setBitsSequence(int)
     */
    public int getBitsSequence() {
        return bitsSequence;
    }

    /**
     * Sets the bit length of sequence component.
     *
     * @param bitsSequence sequence component bit length
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setBitsSequence(int bitsSequence) {
        checkNotNegative(bitsSequence, "sequence bit length must be non-negative");
        this.bitsSequence = bitsSequence;
        return this;
    }

    /**
     * @see #setBitsNodeId(int)
     */
    public int getBitsNodeId() {
        return bitsNodeId;
    }

    /**
     * Sets the bit length of node id component.
     *
     * @param bitsNodeId node id component bit length
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setBitsNodeId(int bitsNodeId) {
        checkNotNegative(bitsNodeId, "node id bit length must be non-negative");
        this.bitsNodeId = bitsNodeId;
        return this;
    }

    /**
     * @see #setAllowedFutureMillis(long)
     */
    public long getAllowedFutureMillis() {
        return allowedFutureMillis;
    }

    /**
     * Sets how far to the future is it allowed to go to generate IDs.
     *
     * @param allowedFutureMillis value in milliseconds
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setAllowedFutureMillis(long allowedFutureMillis) {
        checkNotNegative(allowedFutureMillis, "allowedFutureMillis must be non-negative");
        this.allowedFutureMillis = allowedFutureMillis;
        return this;
    }

    /**
     * @see #setStatisticsEnabled(boolean)
     */
    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    /**
     * Enables or disables statistics gathering of
     * {@link LocalFlakeIdGeneratorStats}.
     *
     * @param statisticsEnabled {@code true} if statistics gathering is enabled
     *                          (which is also the default), {@code false} otherwise
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
        return this;
    }

    @Override
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FlakeIdGeneratorConfig that = (FlakeIdGeneratorConfig) o;

        return prefetchCount == that.prefetchCount
                && prefetchValidityMillis == that.prefetchValidityMillis
                && epochStart == that.epochStart
                && idOffset == that.idOffset
                && nodeIdOffset == that.nodeIdOffset
                && bitsTimestamp == that.bitsTimestamp
                && bitsSequence == that.bitsSequence
                && bitsNodeId == that.bitsNodeId
                && allowedFutureMillis == that.allowedFutureMillis
                && (Objects.equals(name, that.name))
                && statisticsEnabled == that.statisticsEnabled;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{
                name,
                prefetchCount,
                prefetchValidityMillis,
                epochStart,
                idOffset,
                bitsTimestamp,
                bitsSequence,
                bitsNodeId,
                allowedFutureMillis,
                statisticsEnabled});
    }

    @Override
    public String toString() {
        return "FlakeIdGeneratorConfig{"
                + "name='" + name + '\''
                + ", prefetchCount=" + prefetchCount
                + ", prefetchValidityMillis=" + prefetchValidityMillis
                + ", epochStart=" + epochStart
                + ", idOffset=" + idOffset
                + ", nodeIdOffset=" + nodeIdOffset
                + ", bitsTimestamp=" + bitsTimestamp
                + ", bitsSequence=" + bitsSequence
                + ", bitsNodeId=" + bitsNodeId
                + ", allowedFutureMillis=" + allowedFutureMillis
                + ", statisticsEnabled=" + statisticsEnabled
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.FLAKE_ID_GENERATOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(prefetchCount);
        out.writeLong(prefetchValidityMillis);
        out.writeLong(epochStart);
        out.writeLong(idOffset);
        out.writeLong(nodeIdOffset);
        out.writeInt(bitsTimestamp);
        out.writeInt(bitsSequence);
        out.writeInt(bitsNodeId);
        out.writeLong(allowedFutureMillis);
        out.writeBoolean(statisticsEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        prefetchCount = in.readInt();
        prefetchValidityMillis = in.readLong();
        epochStart = in.readLong();
        idOffset = in.readLong();
        nodeIdOffset = in.readLong();
        bitsTimestamp = in.readInt();
        bitsSequence = in.readInt();
        bitsNodeId = in.readInt();
        allowedFutureMillis = in.readLong();
        statisticsEnabled = in.readBoolean();
    }
}
