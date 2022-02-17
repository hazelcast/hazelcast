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

package com.hazelcast.internal.config;

import com.hazelcast.config.WanReplicationRef;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Configuration for Wan target replication reference(read only)
 */
public class WanReplicationRefReadOnly extends WanReplicationRef {

    public WanReplicationRefReadOnly(WanReplicationRef ref) {
        super(ref);
    }

    @Override
    public WanReplicationRef setName(@Nonnull String name) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef setMergePolicyClassName(@Nonnull String mergePolicyClassName) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef setFilters(@Nonnull List<String> filters) {
        throw throwReadOnly();
    }

    @Override
    public WanReplicationRef addFilter(@Nonnull String filterClassName) {
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
