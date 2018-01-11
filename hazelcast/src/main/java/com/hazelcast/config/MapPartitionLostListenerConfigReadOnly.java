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

import com.hazelcast.map.listener.MapPartitionLostListener;

import java.util.EventListener;

/**
 * Read-Only Configuration for MapPartitionLostListener
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class MapPartitionLostListenerConfigReadOnly extends MapPartitionLostListenerConfig {

    public MapPartitionLostListenerConfigReadOnly(MapPartitionLostListenerConfig config) {
        super(config);
    }

    @Override
    public MapPartitionLostListener getImplementation() {
        return (MapPartitionLostListener) implementation;
    }

    @Override
    public ListenerConfig setClassName(String className) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    @Override
    public ListenerConfig setImplementation(EventListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }

    @Override
    public MapPartitionLostListenerConfig setImplementation(MapPartitionLostListener implementation) {
        throw new UnsupportedOperationException("this config is read-only");
    }
}
