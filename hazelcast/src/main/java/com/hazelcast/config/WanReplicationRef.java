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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Configuration for a WAN target replication reference. This configuration
 * is used on IMap and ICache configurations to refer to an actual WAN
 * replication configuration. This way, several maps and cache configurations
 * may share a single WAN replication configuration.
 *
 * @see WanReplicationConfig
 */
public class WanReplicationRef implements IdentifiedDataSerializable, Serializable {

    private boolean republishingEnabled = true;
    private String name;
    private String mergePolicy;
    private List<String> filters = new LinkedList<String>();

    public WanReplicationRef() {
    }

    public WanReplicationRef(WanReplicationRef ref) {
        this(ref.name, ref.mergePolicy, ref.filters, ref.republishingEnabled);
    }

    public WanReplicationRef(String name, String mergePolicy, List<String> filters,
                             boolean republishingEnabled) {
        this.name = name;
        this.mergePolicy = mergePolicy;
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
    public WanReplicationRef setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns the merge policy sent to the WAN replication target to merge
     * replicated entries with existing target entries.
     */
    public String getMergePolicy() {
        return mergePolicy;
    }

    /**
     * Sets the merge policy sent to the WAN replication target to merge
     * replicated entries with existing target entries.
     *
     * @param mergePolicy the merge policy
     * @return this config
     */
    public WanReplicationRef setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    /**
     * Adds the class name implementing the CacheWanEventFilter or
     * MapWanEventFilter for filtering outbound WAN replication events.
     * <p>
     * NOTE: EE only
     *
     * @param filter the class name
     * @return this config
     */
    public WanReplicationRef addFilter(String filter) {
        filters.add(filter);
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
    public List<String> getFilters() {
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
    public WanReplicationRef setFilters(List<String> filters) {
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
        out.writeUTF(name);
        out.writeUTF(mergePolicy);
        out.writeInt(filters.size());
        for (String filter : filters) {
            out.writeUTF(filter);
        }
        out.writeBoolean(republishingEnabled);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        mergePolicy = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            filters.add(in.readUTF());
        }
        republishingEnabled = in.readBoolean();
    }

    @Override
    public String toString() {
        return "WanReplicationRef{"
                + "name='" + name + '\''
                + ", mergePolicy='" + mergePolicy + '\''
                + ", filters='" + filters + '\''
                + ", republishingEnabled='" + republishingEnabled
                + '\''
                + '}';
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WanReplicationRef that = (WanReplicationRef) o;
        if (republishingEnabled != that.republishingEnabled) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (mergePolicy != null ? !mergePolicy.equals(that.mergePolicy) : that.mergePolicy != null) {
            return false;
        }
        return filters != null ? filters.equals(that.filters) : that.filters == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (mergePolicy != null ? mergePolicy.hashCode() : 0);
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        result = 31 * result + (republishingEnabled ? 1 : 0);
        return result;
    }
}
