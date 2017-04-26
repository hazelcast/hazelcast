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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Configuration for Wan target replication reference
 */
@BinaryInterface
public class WanReplicationRef implements DataSerializable, Serializable {

    private String name;
    private String mergePolicy;
    private List<String> filters = new LinkedList<String>();
    private boolean republishingEnabled = true;

    private WanReplicationRefReadOnly readOnly;

    public WanReplicationRef() {
    }

    public WanReplicationRef(String name, String mergePolicy, List<String> filters, boolean republishingEnabled) {
        this.name = name;
        this.mergePolicy = mergePolicy;
        this.filters = filters;
        this.republishingEnabled = republishingEnabled;
    }

    public WanReplicationRef(WanReplicationRef ref) {
        name = ref.name;
        mergePolicy = ref.mergePolicy;
        filters = ref.filters;
        republishingEnabled = ref.republishingEnabled;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return Immutable version of this configuration.
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only.
     */
    public WanReplicationRefReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new WanReplicationRefReadOnly(this);
        }
        return readOnly;
    }

    public String getName() {
        return name;
    }

    public WanReplicationRef setName(String name) {
        this.name = name;
        return this;
    }

    public String getMergePolicy() {
        return mergePolicy;
    }

    public WanReplicationRef setMergePolicy(String mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    /**
     * Add class name implementing the CacheWanEventFilter or MapWanEventFilter for filtering WAN replication events.
     * NOTE: EE only
     *
     * @param filter the class name
     * @return this configuration
     */
    public WanReplicationRef addFilter(String filter) {
        filters.add(filter);
        return this;
    }

    /**
     * Return the list of class names implementing the CacheWanEventFilter or MapWanEventFilter for filtering WAN replication
     * events.
     * NOTE: EE only
     *
     * @return list of class names implementing the CacheWanEventFilter or MapWanEventFilter
     */
    public List<String> getFilters() {
        return filters;
    }

    /**
     * Set the list of class names implementing the CacheWanEventFilter or MapWanEventFilter for filtering WAN replication
     * events.
     * NOTE: EE only
     *
     * @param filters the list of class names implementing CacheWanEventFilter or MapWanEventFilter
     * @return this configuration
     */
    public WanReplicationRef setFilters(List<String> filters) {
        this.filters = filters;
        return this;
    }

    public boolean isRepublishingEnabled() {
        return republishingEnabled;
    }

    public WanReplicationRef setRepublishingEnabled(boolean republishEnabled) {
        this.republishingEnabled = republishEnabled;
        return this;
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
}
