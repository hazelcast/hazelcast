/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction;

import java.util.Properties;

/**
 * An abstract base class for stateless {@link com.hazelcast.cache.impl.eviction.EvictionPolicyStrategy}
 * implementations that just implements empty notification methods, as well as an empty configuration method.
 *
 * @param <A> accessor (key) type of the evictable entry
 * @param <E> {@link com.hazelcast.cache.impl.eviction.Evictable} type (value) of the entry
 */
public abstract class AbstractEvictionPolicyStrategy<A, E extends Evictable>
        implements EvictionPolicyStrategy<A, E> {

    @Override
    public void onCreation(Evictable evictable) {
    }

    @Override
    public void onLoad(Evictable evictable) {
    }

    @Override
    public void onRead(Evictable evictable) {
    }

    @Override
    public void onUpdate(Evictable evictable) {
    }

    @Override
    public void onRemove(Evictable evictable) {
    }

    @Override
    public void configure(Properties properties) {
    }
}
