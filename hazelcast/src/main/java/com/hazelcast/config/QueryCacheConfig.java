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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Contains configuration for {@code QueryCache}.
 *
 * @since 3.5
 */

@SuppressWarnings("checkstyle:methodcount")
public class QueryCacheConfig implements IdentifiedDataSerializable {

    /**
     * By default, after reaching this minimum size, node immediately sends buffered events to {@code QueryCache}.
     */
    public static final int DEFAULT_BATCH_SIZE = 1;

    /**
     * By default, only buffer last {@code DEFAULT_BUFFER_SIZE} events fired from a partition.
     */
    public static final int DEFAULT_BUFFER_SIZE = 16;

    /**
     * Default value of delay seconds which an event wait in the buffer of a node, before sending to {@code QueryCache}.
     */
    public static final int DEFAULT_DELAY_SECONDS = 0;

    /**
     * By default, also cache values of entries besides keys.
     */
    public static final boolean DEFAULT_INCLUDE_VALUE = true;

    /**
     * By default, execute an initial population query prior to creation of the {@code QueryCache}.
     */
    public static final boolean DEFAULT_POPULATE = true;

    /**
     * Default value of coalesce property.
     */
    public static final boolean DEFAULT_COALESCE = false;

    /**
     * Do not serialize given keys by default.
     */
    public static final boolean DEFAULT_SERIALIZE_KEYS = false;

    /**
     * By default, hold values of entries in {@code QueryCache} as binary.
     */
    public static final InMemoryFormat DEFAULT_IN_MEMORY_FORMAT = InMemoryFormat.BINARY;


    /**
     * After reaching this minimum size, node immediately sends buffered events to {@code QueryCache}.
     */
    private int batchSize = DEFAULT_BATCH_SIZE;

    /**
     * Maximum number of events which can be stored in a buffer of partition.
     */
    private int bufferSize = DEFAULT_BUFFER_SIZE;

    /**
     * The minimum number of delay seconds which an event waits in the buffer of node.
     */
    private int delaySeconds = DEFAULT_DELAY_SECONDS;

    /**
     * Flag to enable/disable value caching.
     */
    private boolean includeValue = DEFAULT_INCLUDE_VALUE;

    /**
     * Flag to enable/disable initial population of the {@code QueryCache}.
     */
    private boolean populate = DEFAULT_POPULATE;

    /**
     * Flag to enable/disable coalescing.
     *
     * @see #setCoalesce
     */
    private boolean coalesce = DEFAULT_COALESCE;

    private boolean serializeKeys = DEFAULT_SERIALIZE_KEYS;

    /**
     * Memory format of values of entries in {@code QueryCache}.
     */
    private InMemoryFormat inMemoryFormat = DEFAULT_IN_MEMORY_FORMAT;


    /**
     * The name of {@code QueryCache}.
     */
    private String name;

    /**
     * The predicate to filter events which wil be applied to the {@code QueryCache}.
     */
    private PredicateConfig predicateConfig = new PredicateConfig();

    private EvictionConfig evictionConfig = new EvictionConfig();

    private List<EntryListenerConfig> entryListenerConfigs;

    private List<IndexConfig> indexConfigs;

    public QueryCacheConfig() {
    }

    public QueryCacheConfig(String name) {
        setName(name);
    }

    public QueryCacheConfig(QueryCacheConfig other) {
        this.batchSize = other.batchSize;
        this.bufferSize = other.bufferSize;
        this.delaySeconds = other.delaySeconds;
        this.includeValue = other.includeValue;
        this.populate = other.populate;
        this.coalesce = other.coalesce;
        this.inMemoryFormat = other.inMemoryFormat;
        this.name = other.name;
        this.predicateConfig = other.predicateConfig;
        this.evictionConfig = other.evictionConfig;
        this.entryListenerConfigs = other.entryListenerConfigs;
        this.indexConfigs = other.indexConfigs;
    }

    /**
     * Returns the name of {@code QueryCache}.
     *
     * @return the name of {@code QueryCache}
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of {@code QueryCache}.
     *
     * @param name the name of {@code QueryCache}
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setName(String name) {
        checkHasText(name, "name");

        this.name = name;
        return this;
    }

    /**
     * Returns the predicate of {@code QueryCache}.
     *
     * @return the predicate of {@code QueryCache}
     */
    public PredicateConfig getPredicateConfig() {
        return predicateConfig;
    }

    /**
     * Sets the predicate of {@code QueryCache}.
     *
     * @param predicateConfig config for predicate
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setPredicateConfig(PredicateConfig predicateConfig) {
        this.predicateConfig = checkNotNull(predicateConfig, "predicateConfig can not be null");
        return this;
    }

    /**
     * After reaching this size, node sends buffered events to {@code QueryCache}.
     *
     * @return the batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size which will be used to determine number of events to be sent in a batch to {@code QueryCache}
     *
     * @param batchSize the batch size
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setBatchSize(int batchSize) {
        this.batchSize = checkPositive("batchSize", batchSize);
        return this;
    }

    /**
     * Returns the maximum number of events which can be stored in a buffer of partition.
     *
     * @return the maximum number of events which can be stored in a buffer of partition
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Sets the maximum number of events which can be stored in a buffer of partition.
     *
     * @param bufferSize the buffer size
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setBufferSize(int bufferSize) {
        this.bufferSize = checkPositive("bufferSize", bufferSize);
        return this;
    }

    /**
     * Returns the minimum number of delay seconds which an event waits in the buffer of node
     * before sending to a {@code QueryCache}
     *
     * @return delay seconds
     */
    public int getDelaySeconds() {
        return delaySeconds;
    }

    /**
     * Sets the minimum number of delay seconds which an event waits in the buffer of node
     * before sending to a {@code QueryCache}
     *
     * @param delaySeconds the delay seconds
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setDelaySeconds(int delaySeconds) {
        this.delaySeconds = checkNotNegative(delaySeconds, "delaySeconds");
        return this;
    }

    /**
     * Returns memory format of values of entries in {@code QueryCache}.
     * <p>
     * Default value is binary.
     *
     * @return memory format of values of entries in {@code QueryCache}
     */
    public InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    /**
     * Sets memory format of values of entries in {@code QueryCache}.
     * <p>
     * Default value is binary.
     *
     * @param inMemoryFormat the memory format
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        checkNotNull(inMemoryFormat, "inMemoryFormat cannot be null");
        checkFalse(inMemoryFormat == InMemoryFormat.NATIVE, "InMemoryFormat." + inMemoryFormat + " is not supported.");

        this.inMemoryFormat = inMemoryFormat;
        return this;
    }

    /**
     * Returns {@code true} if value caching enabled, otherwise returns {@code false}.
     * <p>
     * Default value is {@value #DEFAULT_INCLUDE_VALUE}.
     *
     * @return {@code true} if value caching enabled, otherwise returns {@code false}
     */
    public boolean isIncludeValue() {
        return includeValue;
    }

    /**
     * Set {@code true} to enable value caching, otherwise set {@code false}
     * <p>
     * Default value is {@value #DEFAULT_INCLUDE_VALUE}.
     *
     * @param includeValue Set {@code true} if value caching is enabled, otherwise set {@code false}
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    /**
     * Returns {@code true} if initial population of {@code QueryCache} is enabled, otherwise returns {@code false}.
     * <p>
     * Default value is {@value #DEFAULT_POPULATE}.
     *
     * @return {@code true} if initial population of {@code QueryCache} is enabled, otherwise returns {@code false}
     */
    public boolean isPopulate() {
        return populate;
    }

    /**
     * Set {@code true} to enable initial population, otherwise set {@code false}.
     * <p>
     * Default value is {@value #DEFAULT_POPULATE}.
     *
     * @param populate set {@code true} to enable initial population, otherwise set {@code false}
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setPopulate(boolean populate) {
        this.populate = populate;
        return this;
    }

    /**
     * Returns {@code true} if coalescing is is enabled, otherwise returns {@code false}.
     * <p>
     * Default value is {@value #DEFAULT_COALESCE}.
     *
     * @return {@code true} if coalescing is is enabled, otherwise returns {@code false}
     * @see #setCoalesce
     */
    public boolean isCoalesce() {
        return coalesce;
    }

    /**
     * Set {@code true} to enable coalescing, otherwise set {@code false}.
     * This setting is only valid if {@code QueryCacheConfig#delaySeconds} is greater than 0.
     * <p>
     * Default value is {@value #DEFAULT_COALESCE}.
     *
     * @param coalesce set {@code true} to enable, otherwise set {@code false}
     */
    public QueryCacheConfig setCoalesce(boolean coalesce) {
        this.coalesce = coalesce;
        return this;
    }

    /**
     * Checks if the {@link  com.hazelcast.map.QueryCache}
     * key is stored in serialized format or by-reference.
     *
     * @return {@code true} if the key is stored in serialized
     * format, {@code false} if the key is stored by-reference
     */
    public boolean isSerializeKeys() {
        return serializeKeys;
    }

    /**
     * Sets if the {@link  com.hazelcast.map.QueryCache}
     * key is stored in serialized format or by-reference.
     *
     * @param serializeKeys {@code true} if the key is stored in
     *                      serialized format, {@code false} if stored by-reference
     * @return this {@link  QueryCacheConfig} instance
     */
    public QueryCacheConfig setSerializeKeys(boolean serializeKeys) {
        this.serializeKeys = serializeKeys;
        return this;
    }

    /**
     * Returns {@link EvictionConfig} instance for this {@code QueryCache}
     *
     * @return the {@link EvictionConfig} instance for this {@code QueryCache}
     */
    public EvictionConfig getEvictionConfig() {
        return evictionConfig;
    }

    /**
     * Sets the {@link EvictionConfig} instance for this {@code QueryCache}
     *
     * @param evictionConfig the {@link EvictionConfig} instance for eviction configuration to set
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig setEvictionConfig(EvictionConfig evictionConfig) {
        checkNotNull(evictionConfig, "evictionConfig cannot be null");

        this.evictionConfig = evictionConfig;
        return this;
    }

    /**
     * Adds {@link EntryListenerConfig} to this {@code QueryCacheConfig}.
     *
     * @param listenerConfig the {@link EntryListenerConfig} to add
     * @return this {@code QueryCacheConfig} instance
     */
    public QueryCacheConfig addEntryListenerConfig(EntryListenerConfig listenerConfig) {
        checkNotNull(listenerConfig, "listenerConfig cannot be null");

        getEntryListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<EntryListenerConfig> getEntryListenerConfigs() {
        if (entryListenerConfigs == null) {
            entryListenerConfigs = new ArrayList<EntryListenerConfig>();
        }
        return entryListenerConfigs;
    }

    public QueryCacheConfig setEntryListenerConfigs(List<EntryListenerConfig> listenerConfigs) {
        checkNotNull(listenerConfigs, "listenerConfig cannot be null");

        this.entryListenerConfigs = listenerConfigs;
        return this;
    }

    public QueryCacheConfig addIndexConfig(IndexConfig indexConfig) {
        getIndexConfigs().add(indexConfig);
        return this;
    }

    public List<IndexConfig> getIndexConfigs() {
        if (indexConfigs == null) {
            indexConfigs = new ArrayList<>();
        }
        return indexConfigs;
    }

    public QueryCacheConfig setIndexConfigs(List<IndexConfig> indexConfigs) {
        this.indexConfigs = indexConfigs;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.QUERY_CACHE_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(batchSize);
        out.writeInt(bufferSize);
        out.writeInt(delaySeconds);
        out.writeBoolean(includeValue);
        out.writeBoolean(populate);
        out.writeBoolean(coalesce);
        out.writeString(inMemoryFormat.name());
        out.writeString(name);
        out.writeObject(predicateConfig);
        out.writeObject(evictionConfig);
        writeNullableList(entryListenerConfigs, out);
        writeNullableList(indexConfigs, out);
        out.writeBoolean(serializeKeys);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        batchSize = in.readInt();
        bufferSize = in.readInt();
        delaySeconds = in.readInt();
        includeValue = in.readBoolean();
        populate = in.readBoolean();
        coalesce = in.readBoolean();
        inMemoryFormat = InMemoryFormat.valueOf(in.readString());
        name = in.readString();
        predicateConfig = in.readObject();
        evictionConfig = in.readObject();
        entryListenerConfigs = readNullableList(in);
        indexConfigs = readNullableList(in);
        serializeKeys = in.readBoolean();
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryCacheConfig)) {
            return false;
        }

        QueryCacheConfig that = (QueryCacheConfig) o;

        if (batchSize != that.batchSize) {
            return false;
        }
        if (bufferSize != that.bufferSize) {
            return false;
        }
        if (delaySeconds != that.delaySeconds) {
            return false;
        }
        if (includeValue != that.includeValue) {
            return false;
        }
        if (populate != that.populate) {
            return false;
        }
        if (coalesce != that.coalesce) {
            return false;
        }
        if (serializeKeys != that.serializeKeys) {
            return false;
        }
        if (inMemoryFormat != that.inMemoryFormat) {
            return false;
        }
        if (!Objects.equals(name, that.name)) {
            return false;
        }
        if (!Objects.equals(predicateConfig, that.predicateConfig)) {
            return false;
        }
        if (!Objects.equals(evictionConfig, that.evictionConfig)) {
            return false;
        }
        if (!Objects.equals(entryListenerConfigs, that.entryListenerConfigs)) {
            return false;
        }
        return Objects.equals(indexConfigs, that.indexConfigs);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final int hashCode() {
        int result = batchSize;
        result = 31 * result + bufferSize;
        result = 31 * result + delaySeconds;
        result = 31 * result + (includeValue ? 1 : 0);
        result = 31 * result + (populate ? 1 : 0);
        result = 31 * result + (coalesce ? 1 : 0);
        result = 31 * result + (serializeKeys ? 1 : 0);
        result = 31 * result + (inMemoryFormat != null ? inMemoryFormat.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (predicateConfig != null ? predicateConfig.hashCode() : 0);
        result = 31 * result + (evictionConfig != null ? evictionConfig.hashCode() : 0);
        result = 31 * result + (entryListenerConfigs != null ? entryListenerConfigs.hashCode() : 0);
        result = 31 * result + (indexConfigs != null ? indexConfigs.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueryCacheConfig{"
                + "batchSize=" + batchSize
                + ", bufferSize=" + bufferSize
                + ", delaySeconds=" + delaySeconds
                + ", includeValue=" + includeValue
                + ", populate=" + populate
                + ", coalesce=" + coalesce
                + ", serializeKeys=" + serializeKeys
                + ", inMemoryFormat=" + inMemoryFormat
                + ", name='" + name + '\''
                + ", predicateConfig=" + predicateConfig
                + ", evictionConfig=" + evictionConfig
                + ", entryListenerConfigs=" + entryListenerConfigs
                + ", indexConfigs=" + indexConfigs
                + '}';
    }
}
