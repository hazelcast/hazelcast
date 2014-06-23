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

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Contains the configuration for Hazelcast groups.
 * <p/>
 * With groups it is possible to create multiple clusters where each cluster has its own group and doesn't
 * interfere with other clusters.
 */
public final class GroupConfig {

    /**
     * Default group password
     */
    public static final String DEFAULT_GROUP_PASSWORD = "dev-pass";
    /**
     * Default group name
     */
    public static final String DEFAULT_GROUP_NAME = "dev";

    private String name = DEFAULT_GROUP_NAME;
    private String password = DEFAULT_GROUP_PASSWORD;

    /**
     * Creates a GroupConfig with default group-name and group-password.
     */
    public GroupConfig() {
    }

    /**
     * Creates a GroupConfig with the given group-name and default group-password
     *
     * @param name the name of the group
     * @throws IllegalArgumentException if name is null.
     */
    public GroupConfig(final String name) {
        setName(name);
    }

    /**
     * Creates a GroupConfig with the given group-name and group-password
     *
     * @param name     the name of the group
     * @param password the password of the group
     * @throws IllegalArgumentException if name or password is null.
     */
    public GroupConfig(final String name, final String password) {
        setName(name);
        setPassword(password);
    }

    /**
     * Gets the name of the group.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the group name.
     *
     * @param name the name to set
     * @return the updated GroupConfig.
     * @throws IllegalArgumentException if name is null.
     */
    public GroupConfig setName(final String name) {
        this.name = isNotNull(name, "group name");
        return this;
    }

    /**
     * Gets the password to connec to to the group.
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password.
     *
     * @param password the password to set
     * @return the updated GroupConfig.
     * @throws IllegalArgumentException if password is null.
     */
    public GroupConfig setPassword(final String password) {
        this.password = isNotNull(password, "group password");
        return this;
    }

    @Override
    public int hashCode() {
        return (name != null ? name.hashCode() : 0)
                + 31 * (password != null ? password.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof GroupConfig)) {
            return false;
        }
        GroupConfig other = (GroupConfig) obj;
        return (this.name == null ? other.name == null : this.name.equals(other.name))
                && (this.password == null ? other.password == null : this.password.equals(other.password));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("GroupConfig [name=").append(this.name).append(", password=");
        for (int i = 0, len = password.length(); i < len; i++) {
            builder.append('*');
        }
        builder.append("]");
        return builder.toString();
    }
}
