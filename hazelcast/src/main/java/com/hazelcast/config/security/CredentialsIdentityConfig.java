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

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;

/**
 * Identity configuration which just holds given credentials instance.
 */
public class CredentialsIdentityConfig implements IdentityConfig {

    private final Credentials credentials;

    public CredentialsIdentityConfig(Credentials credentials) {
        this.credentials = requireNonNull(credentials);
    }

    public Credentials getCredentials() {
        return credentials;
    }

    @Override
    public IdentityConfig copy() {
        return new CredentialsIdentityConfig(credentials);
    }

    @Override
    public ICredentialsFactory asCredentialsFactory(ClassLoader cl) {
        return new StaticCredentialsFactory(credentials);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CredentialsIdentityConfig [credentials=").append(credentials).append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(credentials);
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
        CredentialsIdentityConfig other = (CredentialsIdentityConfig) obj;
        return Objects.equals(credentials, other.credentials);
    }
}
