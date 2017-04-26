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
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public WanReplicationRef setMergePolicy(String mergePolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public WanReplicationRef setFilters(List<String> filters) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public WanReplicationRef addFilter(String filter) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public WanReplicationRef setRepublishingEnabled(boolean republishingEnabled) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
