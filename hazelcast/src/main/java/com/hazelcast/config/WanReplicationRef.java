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
import com.hazelcast.spi.merge.PassThroughMergePolicy;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for a WAN target replication reference. This configuration
 * is used on IMap and ICache configurations to refer to an actual WAN
 * replication configuration. This way, several maps and cache configurations
 * may share a single WAN replication configuration.
 *
 * @see WanReplicationConfig
 */
public class WanReplicationRef implements IdentifiedDataSerializable, Serializable {

    private static final String DEFAULT_MERGE_POLICY_CLASS_NAME = PassThroughMergePolicy.class.getName();

    private boolean republishingEnabled = true;
    private String name;
    private String mergePolicyClassName = DEFAULT_MERGE_POLICY_CLASS_NAME;
    private List<String> filters = new LinkedList<>();

    public WanReplicationRef() {
    }

    public WanReplicationRef(WanReplicationRef ref) {
        this(ref.name, ref.mergePolicyClassName, ref.filters, ref.republishingEnabled);
    }

    public WanReplicationRef(String name,
                             String mergePolicyClassName,
                             List<String> filters,
                             boolean republishingEnabled) {
        this.name = name;
        this.mergePolicyClassName = mergePolicyClassName;
        this.filters = filters;
        this.republishingEnabled = republishingEnabled;
    }

    /**
     * Returns the WAN replication reference name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the WAN replication reference name.
     *
     * @param name the reference name
     * @return this config
     */
    public WanReplicationRef setName(@Nonnull String name) {
        checkNotNull(name, "Name must not be null");
        this.name = name;
        return this;
    }

    /**
     * Returns the merge policy sent to the WAN replication target to merge
     * replicated entries with existing target entries.
     * The default merge policy is {@link #DEFAULT_MERGE_POLICY_CLASS_NAME}
     */
    public @Nonnull String getMergePolicyClassName() {
        return mergePolicyClassName;
    }

    /**
     * Sets the merge policy sent to the WAN replication target to merge
     * replicated entries with existing target entries.
     * The default merge policy is {@link #DEFAULT_MERGE_POLICY_CLASS_NAME}
     *
     * @param mergePolicyClassName the merge policy
     * @return this config
     */
    public WanReplicationRef setMergePolicyClassName(@Nonnull String mergePolicyClassName) {
        checkNotNull(mergePolicyClassName, "Merge policy class name must not be null");
        this.mergePolicyClassName = mergePolicyClassName;
        return this;
    }

    /**
     * Adds the class name implementing the CacheWanEventFilter or
     * MapWanEventFilter for filtering outbound WAN replication events.
     * <p>
     * NOTE: EE only
     *
     * @param filterClassName the class name
     * @return this config
     */
    public WanReplicationRef addFilter(@Nonnull String filterClassName) {
        checkNotNull(filterClassName, "Filter class name must not be null");
        filters.add(filterClassName);
        return this;
    }

    /**
     * Returns the list of class names implementing the CacheWanEventFilter or
     * MapWanEventFilter for filtering outbound WAN replication events.
     * <p>
     * NOTE: EE only
     *
     * @return list of class names implementing the CacheWanEventFilter or MapWanEventFilter
     */
    public @Nonnull
    List<String> getFilters() {
        return filters;
    }

    /**
     * Sets the list of class names implementing the CacheWanEventFilter or
     * MapWanEventFilter for filtering outbound WAN replication events.
     * <p>
     * NOTE: EE only
     *
     * @param filters the list of class names implementing CacheWanEventFilter or MapWanEventFilter
     * @return this config
     */
    public WanReplicationRef setFilters(@Nonnull List<String> filters) {
        checkNotNull(filters, "Filters must not be null");
        this.filters = filters;
        return this;
    }

    /**
     * Returns {@code true} if incoming WAN events to this member should be
     * republished (forwarded) to this WAN replication reference.
     */
    public boolean isRepublishingEnabled() {
        return republishingEnabled;
    }

    /**
     * Sets if incoming WAN events to this member should be republished
     * (forwarded) to this WAN replication reference.
     *
     * @param republishEnabled whether WAN event republishing is enabled
     * @return this config
     */
    public WanReplicationRef setRepublishingEnabled(boolean republishEnabled) {
        this.republishingEnabled = republishEnabled;
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.WAN_REPLICATION_REF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeString(mergePolicyClassName);
        out.writeInt(filters.size());
        for (String filter : filters) {
            out.writeString(filter);
        }
        out.writeBoolean(republishingEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        mergePolicyClassName = in.readString();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            filters.add(in.readString());
        }
        republishingEnabled = in.readBoolean();
    }

    @Override
    public String toString() {
        return "WanReplicationRef{"
                + "name='" + name + '\''
                + ", mergePolicy='" + mergePolicyClassName + '\''
                + ", filters='" + filters + '\''
                + ", republishingEnabled='" + republishingEnabled
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WanReplicationRef that = (WanReplicationRef) o;
        return republishingEnabled == that.republishingEnabled
                && Objects.equals(name, that.name)
                && Objects.equals(mergePolicyClassName, that.mergePolicyClassName)
                && Objects.equals(filters, that.filters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(republishingEnabled, name, mergePolicyClassName, filters);
    }
}
