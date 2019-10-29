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
     * Maximum value for prefetch count. The limit is ~10% of the time we allow the IDs to be from the future
     * (see {@link com.hazelcast.flakeidgen.impl.FlakeIdGeneratorProxy#ALLOWED_FUTURE_MILLIS}).
     * <p>
     * The reason to limit the prefetch count is that a single call to {@link FlakeIdGenerator#newId()} might
     * be blocked if the future allowance is exceeded: we want to avoid a single call for large batch to block
     * another call for small batch.
     */
    public static final int MAXIMUM_PREFETCH_COUNT = 100000;

    private String name;
    private int prefetchCount = DEFAULT_PREFETCH_COUNT;
    private long prefetchValidityMillis = DEFAULT_PREFETCH_VALIDITY_MILLIS;
    private long idOffset;
    private long nodeIdOffset;
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
        this.idOffset = other.idOffset;
        this.nodeIdOffset = other.nodeIdOffset;
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
                && idOffset == that.idOffset
                && nodeIdOffset == that.nodeIdOffset
                && (name != null ? name.equals(that.name) : that.name == null)
                && statisticsEnabled == that.statisticsEnabled;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{name, prefetchCount, prefetchValidityMillis, idOffset, statisticsEnabled});
    }

    @Override
    public String toString() {
        return "FlakeIdGeneratorConfig{"
                + "name='" + name + '\''
                + ", prefetchCount=" + prefetchCount
                + ", prefetchValidityMillis=" + prefetchValidityMillis
                + ", idOffset=" + idOffset
                + ", nodeIdOffset=" + nodeIdOffset
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
        out.writeLong(idOffset);
        out.writeLong(nodeIdOffset);
        out.writeBoolean(statisticsEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        prefetchCount = in.readInt();
        prefetchValidityMillis = in.readLong();
        idOffset = in.readLong();
        nodeIdOffset = in.readLong();
        statisticsEnabled = in.readBoolean();
    }
}
