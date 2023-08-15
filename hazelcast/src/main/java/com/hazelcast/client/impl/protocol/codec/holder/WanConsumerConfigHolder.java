/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.impl.protocol.codec.holder;

import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Holder used to support the client protocol.
 */
public final class WanConsumerConfigHolder {
    private final boolean persistWanReplicatedData;
    private final String className;
    private final Data implementation;
    private final Map<String, Data> properties;

    public WanConsumerConfigHolder(boolean persistWanReplicatedData,
                                   @Nullable String className,
                                   @Nullable Data implementation,
                                   @Nullable Map<String, Data> properties) {
        this.persistWanReplicatedData = persistWanReplicatedData;
        this.className = className;
        this.properties = properties;
        this.implementation = implementation;
    }

    public boolean isPersistWanReplicatedData() {
        return persistWanReplicatedData;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, Data> getProperties() {
        return properties;
    }

    public Data getImplementation() {
        return implementation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WanConsumerConfigHolder that = (WanConsumerConfigHolder) o;
        return persistWanReplicatedData == that.persistWanReplicatedData && Objects.equals(className, that.className)
                && Objects.equals(implementation, that.implementation) && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(persistWanReplicatedData, className, implementation, properties);
    }
}
