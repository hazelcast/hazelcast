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
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.util.ValidationUtil.hasText;
import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the configuration for a single member group.
 * <p/>
 * See the {@link PartitionGroupConfig} for more information.
 */
public class MemberGroupConfig {

    private final Set<String> interfaces = new HashSet<String>();

    /**
     * Adds an interface to the member group. Duplicate elements are ignored.
     *
     * @param ip the ip address of the interface.
     * @return the updated MemberGroupConfig
     * @throws IllegalArgumentException if ip is null or empty.
     * @see #getInterfaces()
     * @see #clear()
     */
    public MemberGroupConfig addInterface(final String ip) {
        interfaces.add(hasText(ip, "ip"));
        return this;
    }

    /**
     * Removes all interfaces from the member group.
     * <p/>
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
     * @return the collection of interfaces.
     * @see #setInterfaces(java.util.Collection)
     */
    public Collection<String> getInterfaces() {
        return Collections.unmodifiableCollection(interfaces);
    }

    /**
     * Sets the interfaces that are part of a group.
     * <p/>
     * If the interfaces is empty, it will have the same effect as calling {@link #clear()}.
     *
     * @param interfaces the interfaces to set.
     * @return the updated MemberGroupConfig
     * @throws IllegalArgumentException if interfaces is null.
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
    public String toString() {
        final StringBuilder sb = new StringBuilder("MemberGroupConfig{");
        sb.append("interfaces=").append(interfaces);
        sb.append('}');
        return sb.toString();
    }
}
