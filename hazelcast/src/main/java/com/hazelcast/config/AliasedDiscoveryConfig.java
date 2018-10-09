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

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for discovery strategy aliases, e.g. {@literal <gcp>}.
 *
 * @param <T> Subclass that extends {@link AliasedDiscoveryConfig}.
 */
public abstract class AliasedDiscoveryConfig<T extends AliasedDiscoveryConfig<T>> {
    private static final String USE_PUBLIC_IP_PROPERTY = "use-public-ip";
    private static final String ENABLED_PROPERTY = "enabled";

    private final String tag;
    private boolean enabled;
    private boolean usePublicIp;
    private final Map<String, String> properties = new HashMap<String, String>();

    protected AliasedDiscoveryConfig(String tag) {
        this.tag = tag;
    }

    /**
     * Enables or disables the join mechanism based on the given discovery config.
     *
     * @param enabled {@code true} if enabled
     * @return the updated discovery config
     */
    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return (T) this;
    }

    /**
     * Checks whether the given join mechanism is enabled.
     *
     * @return {@code true} if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the property understood by the given SPI Discovery Strategy.
     * <p>
     * Note that it interprets and stores as fields the following properties: "enabled", "use-public-ip".
     *
     * @param name  property name
     * @param value property value
     * @return the updated discovery config
     */
    public T setProperty(String name, String value) {
        if (USE_PUBLIC_IP_PROPERTY.equals(name)) {
            usePublicIp = Boolean.parseBoolean(value);
        } else if (ENABLED_PROPERTY.equals(name)) {
            enabled = Boolean.parseBoolean(value);
        } else {
            properties.put(name, value);
        }
        return (T) this;
    }

    /**
     * Returns property value by the property name.
     *
     * @param name property name
     * @return property value
     */
    public String getProperty(String name) {
        return properties.get(name);
    }

    /**
     * Returns all properties.
     *
     * @return all properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Decides whether the public or private IP should be used to connect to Hazelcast members.
     *
     * @param usePublicIp {@code true} for public IP, {@code false} for private IP
     * @return the updated discovery config
     */
    public T setUsePublicIp(boolean usePublicIp) {
        this.usePublicIp = usePublicIp;
        return (T) this;
    }

    /**
     * Checks whether the public or private IP should be used to connect to Hazelcast members.
     *
     * @return {@true} for public IP, {@code false} for private IP
     */
    public boolean isUsePublicIp() {
        return usePublicIp;
    }

    String getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "AliasedDiscoveryConfig{" + "tag='" + tag + '\'' + ", enabled=" + enabled + ", usePublicIp=" + usePublicIp
                + ", properties=" + properties + '}';
    }
}
