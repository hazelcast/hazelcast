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

import com.hazelcast.config.ServiceConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the Services.
 */
public class ServicesConfig {

    private boolean enableDefaults = true;

    private final Map<String, ServiceConfig> services = new HashMap<>();

    public ServicesConfig() {
    }

    public boolean isEnableDefaults() {
        return enableDefaults;
    }

    public ServicesConfig setEnableDefaults(final boolean enableDefaults) {
        this.enableDefaults = enableDefaults;
        return this;
    }

    public ServicesConfig clear() {
        services.clear();
        return this;
    }

    public Collection<ServiceConfig> getServiceConfigs() {
        return Collections.unmodifiableCollection(services.values());
    }

    public ServicesConfig setServiceConfigs(Collection<ServiceConfig> services) {
        clear();
        for (ServiceConfig service : services) {
            addServiceConfig(service);
        }
        return this;
    }

    public ServicesConfig addServiceConfig(ServiceConfig service) {
        services.put(service.getName(), service);
        return this;
    }

    public ServiceConfig getServiceConfig(String name) {
        return services.get(name);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ServicesConfig)) {
            return false;
        }

        ServicesConfig that = (ServicesConfig) o;

        if (enableDefaults != that.enableDefaults) {
            return false;
        }
        return services.equals(that.services);
    }

    @Override
    public final int hashCode() {
        int result = (enableDefaults ? 1 : 0);
        result = 31 * result + services.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ServicesConfig{enableDefaults=" + enableDefaults + ", services=" + services + '}';
    }
}
