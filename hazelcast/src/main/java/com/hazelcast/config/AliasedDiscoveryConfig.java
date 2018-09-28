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

    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return (T) this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public T setProperty(String key, String value) {
        if (USE_PUBLIC_IP_PROPERTY.equals(key)) {
            usePublicIp = Boolean.parseBoolean(value);
        } else if (ENABLED_PROPERTY.equals(key)) {
            enabled = Boolean.parseBoolean(value);
        } else {
            properties.put(key, value);
        }
        return (T) this;
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public T setUsePublicIp(boolean usePublicIp) {
        this.usePublicIp = usePublicIp;
        return (T) this;
    }

    public boolean isUsePublicIp() {
        return usePublicIp;
    }

    String getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return "AliasedDiscoveryConfig{" + "enabled=" + enabled + ", usePublicIp=" + usePublicIp + ", properties=" + properties
                + '}';
    }
}
