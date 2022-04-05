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

package com.hazelcast.config.security;

import java.util.Objects;

import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

/**
 * Simple username/password identity configuration.
 */
public class UsernamePasswordIdentityConfig implements IdentityConfig {

    private final String username;
    private final String password;

    public UsernamePasswordIdentityConfig(String name, String password) {
        this.username = name;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public ICredentialsFactory asCredentialsFactory(ClassLoader cl) {
        return new StaticCredentialsFactory(new UsernamePasswordCredentials(username, password));
    }

    @Override
    public IdentityConfig copy() {
        return new UsernamePasswordIdentityConfig(username, password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(password, username);
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
        UsernamePasswordIdentityConfig other = (UsernamePasswordIdentityConfig) obj;
        return Objects.equals(password, other.password) && Objects.equals(username, other.username);
    }

    @Override
    public String toString() {
        return "UsernamePasswordIdentityConfig [username=" + username + ", password=***]";
    }
}
