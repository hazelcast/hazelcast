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
 */
public class AliasedDiscoveryConfig {
    private boolean enabled;
    private String environment;
    private final Map<String, String> properties = new HashMap<String, String>();

    public AliasedDiscoveryConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public AliasedDiscoveryConfig addProperty(String key, String value) {
        properties.put(key, value);
        return this;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getEnvironment() {
        return environment;
    }

    public AliasedDiscoveryConfig setEnvironment(String environment) {
        this.environment = environment;
        return this;
    }

    @Override
    public String toString() {
        return "AliasedDiscoveryConfig{" + "enabled=" + enabled + ", environment='" + environment + '\'' + ", properties="
                + properties + '}';
    }
}
