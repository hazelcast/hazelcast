/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
/**
 * Configuration for Wan target replication reference
 */
public class WanReplicationRef {
    private String name;
    private String mergePolicy;

    private WanReplicationRefReadOnly readOnly;

    public WanReplicationRef() {
    }

    public WanReplicationRef(String name, String mergePolicy) {
        this.name = name;
        this.mergePolicy = mergePolicy;
    }

    public WanReplicationRef(WanReplicationRef ref) {
        name = ref.name;
        mergePolicy = ref.mergePolicy;
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

    @Override
    public String toString() {
        return "WanReplicationRef{"
                + "name='" + name + '\''
                + ", mergePolicy='" + mergePolicy
                + '\''
                + '}';
    }
}
