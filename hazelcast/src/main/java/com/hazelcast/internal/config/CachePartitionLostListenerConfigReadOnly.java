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

import com.hazelcast.cache.impl.event.CachePartitionLostListener;
import com.hazelcast.config.CachePartitionLostListenerConfig;
import com.hazelcast.config.ListenerConfig;

import javax.annotation.Nonnull;
import java.util.EventListener;

/**
 * Read-Only Configuration for CachePartitionLostListener
 *
 * @see CachePartitionLostListener
 */
public class CachePartitionLostListenerConfigReadOnly
        extends CachePartitionLostListenerConfig {

    public CachePartitionLostListenerConfigReadOnly(CachePartitionLostListenerConfig config) {
        super(config);
    }

    @Override
    public CachePartitionLostListener getImplementation() {
        return (CachePartitionLostListener) implementation;
    }

    @Override
    public ListenerConfig setClassName(@Nonnull String className) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    @Override
    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    @Override
    public CachePartitionLostListenerConfig setImplementation(CachePartitionLostListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }
}
