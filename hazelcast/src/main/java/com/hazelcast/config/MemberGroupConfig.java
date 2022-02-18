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

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * Contains the configuration for a single member group.
 * <p>
 * See the {@link PartitionGroupConfig} for more information.
 */
public class MemberGroupConfig {

    private final Set<String> interfaces = new HashSet<String>();

    /**
     * Adds an interface to the member group. Duplicate elements are ignored.
     *
     * @param ip the IP address of the interface
     * @return the updated MemberGroupConfig
     * @throws IllegalArgumentException if IP is {@code null} or empty
     * @see #getInterfaces()
     * @see #clear()
     */
    public MemberGroupConfig addInterface(String ip) {
        interfaces.add(checkHasText(ip, "ip must contain text"));
        return this;
    }

    /**
     * Removes all interfaces from the member group.
     * <p>
     * If no members are in the group, the call is ignored.
     *
     * @return the updated MemberGroupConfig
     */
    public MemberGroupConfig clear() {
        interfaces.clear();
        return this;
    }

    /**
     * Gets an unmodifiable collection containing all interfaces.
     *
     * @return the unmodifiable collection containing all interfaces
     * @see #setInterfaces(java.util.Collection)
     */
    public Collection<String> getInterfaces() {
        return Collections.unmodifiableCollection(interfaces);
    }

    /**
     * Sets the interfaces that are part of a group.
     * <p>
     * If the interfaces is empty, it will have the same effect as calling {@link #clear()}.
     *
     * @param interfaces the interfaces to set that are part of a group
     * @return the updated MemberGroupConfig
     * @throws IllegalArgumentException if interfaces is {@code null}
     * @see #getInterfaces()
     * @see #clear()
     */
    public MemberGroupConfig setInterfaces(final Collection<String> interfaces) {
        isNotNull(interfaces, "interfaces");
        clear();
        this.interfaces.addAll(interfaces);
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof MemberGroupConfig)) {
            return false;
        }

        MemberGroupConfig that = (MemberGroupConfig) o;

        return interfaces.equals(that.interfaces);
    }

    @Override
    public final int hashCode() {
        return interfaces.hashCode();
    }

    @Override
    public String toString() {
        return "MemberGroupConfig{interfaces=" + interfaces + '}';
    }
}
