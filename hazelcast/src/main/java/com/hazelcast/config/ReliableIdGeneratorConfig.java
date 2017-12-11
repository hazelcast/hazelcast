/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.reliableidgen.ReliableIdGenerator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Preconditions;

import java.io.IOException;

/**
 * The {@code ReliableIdGeneratorConfig} contains the configuration for the member
 * regarding {@link com.hazelcast.core.HazelcastInstance#getReliableIdGenerator(String)
 * Reliable ID Generator}.
 * <p>
 * Settings here only apply when ID generator is used from member, not when clients
 * connect to this member to generate IDs - each client has its own settings in {@code
 * ClientConfig}.
 */
public class ReliableIdGeneratorConfig implements IdentifiedDataSerializable {

    /**
     * Default value for {@link #getPrefetchCount()}.
     */
    public static final int DEFAULT_PREFETCH_COUNT = 100;

    /**
     * Default value for {@link #getPrefetchValidityMillis()}.
     */
    public static final long DEFAULT_PREFETCH_VALIDITY_MILLIS = 10000;

    private String name;
    private int prefetchCount = DEFAULT_PREFETCH_COUNT;
    private long prefetchValidityMillis = DEFAULT_PREFETCH_VALIDITY_MILLIS;

    private transient ReliableIdGeneratorConfigReadOnly readOnly;

    // for deserialization
    ReliableIdGeneratorConfig() {
    }

    public ReliableIdGeneratorConfig(String name) {
        this.name = name;
    }

    /**
     * Copy-constructor
     */
    public ReliableIdGeneratorConfig(ReliableIdGeneratorConfig other) {
        this.name = other.name;
        this.prefetchCount = other.prefetchCount;
        this.prefetchValidityMillis = other.prefetchValidityMillis;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public ReliableIdGeneratorConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ReliableIdGeneratorConfigReadOnly(this);
        }
        return readOnly;
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
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @see #setPrefetchCount(int)
     */
    public int getPrefetchCount() {
        return Math.max(1, prefetchCount);
    }

    /**
     * How many IDs are pre-fetched on the background when one call to
     * {@link ReliableIdGenerator#newId()} is made.
     * <p>
     * Value must be >= 1, default is 100.
     *
     * @return this instance for fluent API
     */
    public ReliableIdGeneratorConfig setPrefetchCount(int prefetchCount) {
        Preconditions.checkPositive(prefetchCount, "prefetch-count must be >=1, not " + prefetchCount);
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
     * For how long the pre-fetched IDs can be used. If this time elapses, new IDs will be fetched.
     * <p>
     * Time unit is milliseconds.
     * <p>
     * If value is &lt;= 0, validity is unlimited. Default value is 10000 (10 seconds).
     * <p>
     * The IDs contain timestamp component, which ensures rough global ordering of IDs. If an ID
     * is assigned to an event that occurred much later, it will be much out of order. If you don't need
     * ordering, set this value to 0.
     *
     * @return this instance for fluent API
     */
    public ReliableIdGeneratorConfig setPrefetchValidityMillis(long prefetchValidityMs) {
        this.prefetchValidityMillis = prefetchValidityMs;
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

        ReliableIdGeneratorConfig that = (ReliableIdGeneratorConfig) o;

        if (prefetchCount != that.prefetchCount) {
            return false;
        }
        if (prefetchValidityMillis != that.prefetchValidityMillis) {
            return false;
        }
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + prefetchCount;
        result = 31 * result + (int) (prefetchValidityMillis ^ (prefetchValidityMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ReliableIdGeneratorConfig{"
                + "name='" + name + '\''
                + ", prefetchCount=" + prefetchCount
                + ", prefetchValidityMillis=" + prefetchValidityMillis
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.RELIABLE_ID_GENERATOR_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(prefetchCount);
        out.writeLong(prefetchValidityMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        prefetchCount = in.readInt();
        prefetchValidityMillis = in.readLong();
    }
}
