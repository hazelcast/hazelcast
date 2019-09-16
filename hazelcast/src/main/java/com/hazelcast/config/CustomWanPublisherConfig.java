/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Configuration object for a custom WAN publisher. A single publisher defines
 * how WAN events are sent to a specific publisher.
 * The publisher can be some other external system which is not a Hazelcast
 * cluster (e.g. JMS queue).
 */
public class CustomWanPublisherConfig extends AbstractWanPublisherConfig {

    @Override
    public String toString() {
        return "CustomWanPublisherConfig{"
                + "publisherId='" + publisherId + '\''
                + ", className='" + className + '\''
                + ", implementation=" + implementation
                + ", properties=" + properties
                + '}';
    }

    @Override
    public @Nonnull
    String getPublisherId() {
        return super.getPublisherId();
    }

    @Override
    public CustomWanPublisherConfig setPublisherId(@Nonnull String publisherId) {
        super.setPublisherId(checkNotNull(publisherId, "Publisher ID must not be null"));
        return this;
    }

    @Override
    public CustomWanPublisherConfig setProperties(@Nonnull Map<String, Comparable> properties) {
        super.setProperties(properties);
        return this;
    }

    @Override
    public CustomWanPublisherConfig setClassName(String className) {
        super.setClassName(className);
        return this;
    }

    @Override
    public CustomWanPublisherConfig setImplementation(Object implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.WAN_CUSTOM_PUBLISHER_CONFIG;
    }
}
