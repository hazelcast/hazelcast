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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.map.listener.MapPartitionLostListener;

/**
 * Configuration for MapPartitionLostListener
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostListenerConfig extends ListenerConfig {

    public MapPartitionLostListenerConfig() {
    }

    public MapPartitionLostListenerConfig(String className) {
        super(className);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListener implementation) {
        super(implementation);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListenerConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    @Override
    public MapPartitionLostListener getImplementation() {
        return (MapPartitionLostListener) implementation;
    }

    public MapPartitionLostListenerConfig setImplementation(final MapPartitionLostListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MapPartitionLostListenerConfig that = (MapPartitionLostListenerConfig) o;

        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        return !(implementation != null ? !implementation.equals(that.implementation) : that.implementation != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        return result;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.MAP_PARTITION_LOST_LISTENER_CONFIG;
    }
}
