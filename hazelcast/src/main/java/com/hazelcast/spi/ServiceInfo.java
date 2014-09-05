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

package com.hazelcast.spi;

/**
 * Contains the name of the service and the actual service.
 *
 * @author mdogan 6/8/13
 */
public final class ServiceInfo {

    private final String name;
    private final Object service;

    public ServiceInfo(String name, Object service) {
        this.name = name;
        this.service = service;
    }

    public String getName() {
        return name;
    }

    public Object getService() {
        return service;
    }

    public boolean isCoreService() {
        return service instanceof CoreService;
    }

    public boolean isManagedService() {
        return service instanceof ManagedService;
    }

    public boolean isConfigurableService() {
        return service instanceof ConfigurableService;
    }

    @SuppressWarnings("unchecked")
    public boolean isInstanceOf(Class type) {
        return type.isAssignableFrom(service.getClass());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServiceInfo that = (ServiceInfo) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServiceInfo{");
        sb.append("name='").append(name).append('\'');
        sb.append(", service=").append(service);
        sb.append('}');
        return sb.toString();
    }
}
