/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.Preconditions.isNotNull;
import static java.util.Collections.newSetFromMap;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains the configuration for Hazelcast Management Center.
 */
public final class ManagementCenterConfig implements TrustedInterfacesConfigurable<ManagementCenterConfig> {

    private boolean scriptingEnabled;

    private final Set<String> trustedInterfaces = newSetFromMap(new ConcurrentHashMap<>());

    public ManagementCenterConfig() {
    }

    /**
     * Enables/disables scripting on the member. Management center allows to send a script for execution to a member. The script
     * can access the underlying system Hazelcast member is running on with permissions of user running the member. Disabling
     * scripting improves the member security.
     * <p>
     * Default value for this config element is {@code false}.
     *
     * @param scriptingEnabled {@code true} to allow scripting on the member, {@code false} to disallow
     * @return this management center config instance
     * @since 3.12
     */
    public ManagementCenterConfig setScriptingEnabled(final boolean scriptingEnabled) {
        this.scriptingEnabled = scriptingEnabled;
        return this;
    }

    /**
     * Returns if scripting is allowed ({@code true}) or disallowed ({@code false}).
     */
    public boolean isScriptingEnabled() {
        return scriptingEnabled;
    }

    /**
     * Gets the trusted interfaces.
     *
     * @return the trusted interfaces
     * @see #setTrustedInterfaces(java.util.Set)
     */
    public Set<String> getTrustedInterfaces() {
        return trustedInterfaces;
    }

    /**
     * Sets the trusted interfaces.
     * <p>
     * The interface is an IP address where the last octet can be a wildcard '*' or a range '10-20'.
     *
     * @param interfaces the new trusted interfaces
     * @return the updated MulticastConfig
     * @see IllegalArgumentException if interfaces is {@code null}
     */
    public ManagementCenterConfig setTrustedInterfaces(Set<String> interfaces) {
        isNotNull(interfaces, "interfaces");

        trustedInterfaces.clear();
        trustedInterfaces.addAll(interfaces);
        return this;
    }

    /**
     * Adds a trusted interface.
     *
     * @param ip the IP of the trusted interface
     * @throws IllegalArgumentException if IP is {@code null}
     * @see #setTrustedInterfaces(java.util.Set)
     */
    public ManagementCenterConfig addTrustedInterface(final String ip) {
        trustedInterfaces.add(isNotNull(ip, "ip"));
        return this;
    }

    @Override
    public String toString() {
        return "ManagementCenterConfig{"
                + "scriptingEnabled=" + scriptingEnabled
                + "trustedInterfaces=" + trustedInterfaces
                + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hash(scriptingEnabled, trustedInterfaces);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ManagementCenterConfig other = (ManagementCenterConfig) obj;
        return scriptingEnabled == other.scriptingEnabled && Objects.equals(trustedInterfaces, other.trustedInterfaces);
    }
}
