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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
/**
 * Contains the configuration for Interfaces.
 */
public class InterfacesConfig {

    private boolean enabled;

    private final Set<String> interfaceSet = new HashSet<String>();

    /**
     * @return {@code true} if the interface is enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled interface to set
     */
    public InterfacesConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Adds a new interface
     *
     * @param ip the added interface
     */
    public InterfacesConfig addInterface(final String ip) {
        interfaceSet.add(ip);
        return this;
    }

    /**
     * clears all interfaces.
     */
    public InterfacesConfig clear() {
        interfaceSet.clear();
        return this;
    }

    /**
     * @return a read-only collection of interfaces
     */
    public Collection<String> getInterfaces() {
        return Collections.unmodifiableCollection(interfaceSet);
    }

    /**
     * Adds a collection of interfaces.
     * Clears the current collection and then adds all entries of new collection.
     *
     * @param interfaces the interfaces to set
     */
    public InterfacesConfig setInterfaces(final Collection<String> interfaces) {
        clear();
        this.interfaceSet.addAll(interfaces);
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof InterfacesConfig)) {
            return false;
        }

        InterfacesConfig that = (InterfacesConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        return interfaceSet.equals(that.interfaceSet);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + interfaceSet.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "InterfacesConfig{enabled=" + enabled + ", interfaces=" + interfaceSet + '}';
    }
}
