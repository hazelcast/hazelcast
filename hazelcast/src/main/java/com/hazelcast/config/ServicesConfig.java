/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mdogan 9/18/12
 */
public class ServicesConfig {

    private boolean enableDefaults = true;

    private final Map<String, ServiceConfig> services = new HashMap<String, ServiceConfig>();

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
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServicesConfig{");
        sb.append("enableDefaults=").append(enableDefaults);
        sb.append(", services=").append(services);
        sb.append('}');
        return sb.toString();
    }
}
