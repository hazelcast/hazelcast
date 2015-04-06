/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;

/**
 * Configuration for Wan target replication reference
 */
public class WanReplicationRef implements DataSerializable, Serializable {

    private String name;
    private String mergePolicy;
    private boolean republishEnabled;

    private WanReplicationRefReadOnly readOnly;

    public WanReplicationRef() {
    }

    public WanReplicationRef(String name, String mergePolicy, boolean republishEnabled) {
        this.name = name;
        this.mergePolicy = mergePolicy;
        this.republishEnabled = republishEnabled;
    }

    public WanReplicationRef(WanReplicationRef ref) {
        name = ref.name;
        mergePolicy = ref.mergePolicy;
        republishEnabled = ref.republishEnabled;
    }

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

    public boolean isRepublishEnabled() {
        return republishEnabled;
    }

    public WanReplicationRef setRepublishEnabled(boolean republishEnabled) {
        this.republishEnabled = republishEnabled;
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(mergePolicy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        mergePolicy = in.readUTF();
    }

    @Override
    public String toString() {
        return "WanReplicationRef{"
                + "name='" + name + '\''
                + ", mergePolicy='" + mergePolicy
                + '\''
                + '}';
    }
}
