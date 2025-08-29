/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.security;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;

/**
 * Typed authentication configuration for {@code SimplePropertiesLoginModule}. The user configuration (username, password, and
 * assigned roles) is directly part of the configuration object.
 */
public class SimpleAuthenticationConfig extends AbstractClusterLoginConfig<SimpleAuthenticationConfig> {

    private final Map<String, UserDto> userMap = new ConcurrentHashMap<>();
    private volatile String roleSeparator;

    /**
     * Adds one user to the configuration.
     *
     * @param password user's password
     * @param roles roles assigned to the user
     * @return this object
     */
    public SimpleAuthenticationConfig addUser(@Nonnull String username, @Nonnull String password, String... roles) {
        addUser(username, new UserDto(password, roles));
        return self();
    }

    /**
     * Adds one user to the configuration.
     *
     * @param userDto user's attributes
     * @return this object
     */
    public SimpleAuthenticationConfig addUser(@Nonnull String username, @Nonnull UserDto userDto) {
        requireNonNull(userDto, "UserDto object has to be provided");
        userMap.compute(requireNonNull(username), (u, dto) -> {
            if (dto != null) {
                throw new IllegalArgumentException("User " + username + " already exists.");
            }
            return userDto;
        });
        return self();
    }

    /**
     * Allows to use a custom role separator in the configuration. It's important in case when a role name contains the default
     * separator "," (comma).
     *
     * @param roleSeparator new separator
     * @return this object
     */
    public SimpleAuthenticationConfig setRoleSeparator(@Nullable String roleSeparator) {
        if (roleSeparator != null && roleSeparator.isEmpty()) {
            throw new IllegalArgumentException("Empty role separator is not allowed");
        }
        this.roleSeparator = roleSeparator;
        return self();
    }

    /**
     * Returns the custom role separator (when set). If no custom one is used, {@code null} is returned and default separator is
     * used ("," - comma).
     *
     * @return the separator
     */
    public @Nullable String getRoleSeparator() {
        return roleSeparator;
    }

    /**
     * Returns names of users registered in the configuration.
     *
     * @return set of usernames
     */
    public @Nonnull Set<String> getUsernames() {
        return new HashSet<>(userMap.keySet());
    }

    /**
     * Return the configured password for given username.
     *
     * @return user's password or null if such user doesn't exist
     */
    public String getPassword(@Nonnull String username) {
        UserDto user = userMap.get(requireNonNull(username, "Username has to be provided"));
        return user == null ? null : user.password;
    }

    /**
     * Returns role names assigned to the given user.
     *
     * @return configured roles or null if the user doesn't exist
     */
    public Set<String> getRoles(@Nonnull String username) {
        UserDto user = userMap.get(requireNonNull(username, "Username has to be provided"));
        return user == null ? null : new HashSet<>(user.roles);
    }

    /**
     * Replaces the users with ones from the given map.
     *
     * @param map map of new users
     * @return this object
     */
    public SimpleAuthenticationConfig setUserMap(@Nullable Map<String, UserDto> map) {
        userMap.clear();
        if (map != null) {
            userMap.putAll(map);
        }
        return self();
    }

    @Override
    protected Properties initLoginModuleProperties() {
        Properties props = super.initLoginModuleProperties();
        setIfConfigured(props, "roleSeparator", roleSeparator);
        String rs = roleSeparator != null ? roleSeparator : ",";
        for (Map.Entry<String, UserDto> entry : userMap.entrySet()) {
            String name = entry.getKey();
            UserDto user = entry.getValue();
            setIfConfigured(props, "password." + name, user.password);
            setIfConfigured(props, "roles." + name, user.roles.stream().collect(joining(rs)));
        }
        return props;
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig(
                "com.hazelcast.security.loginimpl.SimplePropertiesLoginModule", LoginModuleUsage.REQUIRED);

        loginModuleConfig.setProperties(initLoginModuleProperties());

        return new LoginModuleConfig[] { loginModuleConfig };
    }

    @Override
    public String toString() {
        return "SimpleAuthenticationConfig [userMap=" + userMap + ", roleSeparator=" + roleSeparator + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(roleSeparator, userMap);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SimpleAuthenticationConfig other = (SimpleAuthenticationConfig) obj;
        return Objects.equals(roleSeparator, other.roleSeparator) && Objects.equals(userMap, other.userMap);
    }

    @Override
    protected SimpleAuthenticationConfig self() {
        return this;
    }

    /**
     * Helper immutable object representing user attributes (i.e. password and roles) in the {@link SimpleAuthenticationConfig}.
     */
    public static class UserDto {
        final String password;
        final Set<String> roles;

        public UserDto(@Nonnull String password, @Nonnull String... roles) {
            this.password = requireNonEmpty(password, "Password can't be empty");
            requireNonNull(roles, "Roles can't be null");
            this.roles = Set.of(roles);
        }

        @Override
        public String toString() {
            return "{password=***, roles=" + roles + "}";
        }

        @Override
        public int hashCode() {
            return Objects.hash(password, roles);
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
            UserDto other = (UserDto) obj;
            return Objects.equals(password, other.password) && Objects.equals(roles, other.roles);
        }
    }

    protected static String requireNonEmpty(String str, String message) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return str;
    }
}
