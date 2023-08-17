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

import java.util.Map;
import java.util.Objects;

public final class WanCustomPublisherConfigHolder {
    private final String publisherId;
    private final String className;
    private final Data implementation;
    private final Map<String, Data> properties;

    public WanCustomPublisherConfigHolder(String publisherId, String className, Data implementation, Map<String, Data> properties) {
        this.publisherId = publisherId;
        this.className = className;
        this.implementation = implementation;
        this.properties = properties;
    }

    public String getPublisherId() {
        return publisherId;
    }

    public String getClassName() {
        return className;
    }

    public Data getImplementation() {
        return implementation;
    }

    public Map<String, Data> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WanCustomPublisherConfigHolder holder = (WanCustomPublisherConfigHolder) o;
        return Objects.equals(publisherId, holder.publisherId) && Objects.equals(className, holder.className) && Objects.equals(
                implementation, holder.implementation) && Objects.equals(properties, holder.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publisherId, className, implementation, properties);
    }
}
