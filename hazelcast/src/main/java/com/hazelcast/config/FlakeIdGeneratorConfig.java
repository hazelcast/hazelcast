/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
     * Default value for {@link #getEpochStart()}. {@code 1514764800000} is the value {@code
     * System.currentTimeMillis()} would return on 1.1.2018 0:00 UTC.
     */
    public static final long DEFAULT_EPOCH_START = 1514764800000L;

    /**
     * Default value for {@link #getBitsSequence()}.
     */
    public static final int DEFAULT_BITS_SEQUENCE = 6;

    /**
     * Default value for {@link #getBitsNodeId()}.
     */
    public static final int DEFAULT_BITS_NODE_ID = 16;

    /**
     * Default value for {@link #getAllowedFutureMillis()}.
     */
    public static final long DEFAULT_ALLOWED_FUTURE_MILLIS = 15000;

    /**
     * Maximum value for prefetch count. The limit is ~10% of the default time we allow the IDs to be from the future
     * (see {@link #DEFAULT_ALLOWED_FUTURE_MILLIS}).
     * <p>
     * The reason to limit the prefetch count is that a single call to {@link FlakeIdGenerator#newId()} might
     * be blocked if the future allowance is exceeded: we want to avoid a single call for a large batch to block
     * another call for a small batch.
     */
    public static final int MAXIMUM_PREFETCH_COUNT = 100000;

    private static final int MAX_BITS = 63;

    private String name;
    private int prefetchCount = DEFAULT_PREFETCH_COUNT;
    private long prefetchValidityMillis = DEFAULT_PREFETCH_VALIDITY_MILLIS;
    private long epochStart = DEFAULT_EPOCH_START;
    private long nodeIdOffset;
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
        this.nodeIdOffset = other.nodeIdOffset;
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
     * This setting pertains only to {@link FlakeIdGenerator#newId newId()} calls made on the member
     * that configured it.
     *
     * @param prefetchCount the desired prefetch count, in the range 1..{@value #MAXIMUM_PREFETCH_COUNT}.
     * @return this instance for fluent API
     */
    public FlakeIdGeneratorConfig setPrefetchCount(int prefetchCount) {
        checkTrue(prefetchCount > 0 && prefetchCount <= MAXIMUM_PREFETCH_COUNT,
                "prefetch-count must be 1.." + MAXIMUM_PREFETCH_COUNT + ", but is " + prefetchCount);
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
     * Sets the offset of timestamp component in milliseconds. By default it's {@value
     * DEFAULT_EPOCH_START}, that is the beginning of 2018. You can adjust the value to determine the
     * lifespan of the generator. See {@link FlakeIdGenerator}'s class javadoc for more information.
     * <p>
     * <i>Note:</i> If you set the epoch start to a future instant, negative IDs will be generated
     * until that time occurs.
     *
     * @param epochStart the desired epoch start
     * @return this instance for fluent API
     * @since 4.0
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
     * @see #setBitsSequence(int)
     */
    public int getBitsSequence() {
        return bitsSequence;
    }

    /**
     * Sets the bit-length of the sequence component. This setting determines the maximum rate at
     * which IDs can be generated. See {@link FlakeIdGenerator}'s class javadoc for more
     * information.
     *
     * @param bitsSequence sequence component bit-length
     * @return this instance for fluent API
     * @since 4.0
     */
    public FlakeIdGeneratorConfig setBitsSequence(int bitsSequence) {
        checkTrue(bitsSequence >= 0 && bitsSequence < MAX_BITS, "sequence bit-length must be 0.." + MAX_BITS);
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
     * Sets the bit-length of node id component. See {@link FlakeIdGenerator}'s class javadoc for
     * more information.
     *
     * @param bitsNodeId node id component bit-length
     * @return this instance for fluent API
     * @since 4.0
     */
    public FlakeIdGeneratorConfig setBitsNodeId(int bitsNodeId) {
        checkTrue(bitsNodeId >= 0 && bitsNodeId < MAX_BITS, "node ID bit-length must be 0.." + MAX_BITS);
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
     * Sets how far to the future is the generator allowed to go to generate IDs without blocking.
     * <p>
     * The number of bits configured for the sequence number ({@link #setBitsSequence(int)}
     * determines how many IDs can be generated per second. We allow the generator to generate IDs
     * with future timestamps, and this settings limits how much. When more IDs are requested, the
     * call will block. This is important in case of a cluster black-out or cluster restart: we
     * don't store how far the members went and after they restart, they will start from current
     * time. If before the restart the generator went beyond the current time, duplicate IDs could
     * be generated.
     * <p>
     * The default value is 15 seconds (15000). If your cluster is able to restart more quickly, set
     * a lower value.
     * <p>
     * See {@link FlakeIdGenerator}'s class javadoc for more information.
     *
     * @param allowedFutureMillis value in milliseconds
     * @return this instance for fluent API
     * @since 4.0
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
     * Enables or disables statistics gathering of {@link LocalFlakeIdGeneratorStats}.
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
                && nodeIdOffset == that.nodeIdOffset
                && bitsSequence == that.bitsSequence
                && bitsNodeId == that.bitsNodeId
                && allowedFutureMillis == that.allowedFutureMillis
                && Objects.equals(name, that.name)
                && statisticsEnabled == that.statisticsEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name,
                prefetchCount,
                prefetchValidityMillis,
                epochStart,
                nodeIdOffset,
                bitsSequence,
                bitsNodeId,
                allowedFutureMillis,
                statisticsEnabled);
    }

    @Override
    public String toString() {
        return "FlakeIdGeneratorConfig{"
                + "name='" + name + '\''
                + ", prefetchCount=" + prefetchCount
                + ", prefetchValidityMillis=" + prefetchValidityMillis
                + ", epochStart=" + epochStart
                + ", nodeIdOffset=" + nodeIdOffset
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
        out.writeString(name);
        out.writeInt(prefetchCount);
        out.writeLong(prefetchValidityMillis);
        out.writeLong(epochStart);
        out.writeLong(nodeIdOffset);
        out.writeInt(bitsSequence);
        out.writeInt(bitsNodeId);
        out.writeLong(allowedFutureMillis);
        out.writeBoolean(statisticsEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        prefetchCount = in.readInt();
        prefetchValidityMillis = in.readLong();
        epochStart = in.readLong();
        nodeIdOffset = in.readLong();
        bitsSequence = in.readInt();
        bitsNodeId = in.readInt();
        allowedFutureMillis = in.readLong();
        statisticsEnabled = in.readBoolean();
    }
}
