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

import com.hazelcast.internal.eviction.EvictionPolicyComparator;
import com.hazelcast.nio.serialization.BinaryInterface;

/**
 * Read only version of {@link com.hazelcast.config.EvictionConfig}.
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
@BinaryInterface
public class EvictionConfigReadOnly extends EvictionConfig {

    public EvictionConfigReadOnly(EvictionConfig config) {
        super(config);
    }

    @Override
    public EvictionConfigReadOnly setSize(int size) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public EvictionConfigReadOnly setMaximumSizePolicy(MaxSizePolicy maxSizePolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public EvictionConfigReadOnly setEvictionPolicy(EvictionPolicy evictionPolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public EvictionConfig setComparatorClassName(String comparatorClassName) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public EvictionConfig setComparator(EvictionPolicyComparator comparator) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
