/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.BinaryInterface;

import java.util.List;

/**
 * Configuration for Wan target replication reference(read only)
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
@BinaryInterface
public class WanReplicationRefReadOnly extends WanReplicationRef {

    public WanReplicationRefReadOnly(WanReplicationRef ref) {
        super(ref);
    }

    @Override
    public WanReplicationRef setName(String name) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef setMergePolicy(String mergePolicy) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef setFilters(List<String> filters) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef addFilter(String filter) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef setRepublishingEnabled(boolean republishingEnabled) {
        throw throwReadOnly();
    }

    private UnsupportedOperationException throwReadOnly() {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
