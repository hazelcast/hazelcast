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

package com.hazelcast.client.config;

import com.hazelcast.security.Credentials;

/**
 * Contains the security configuration for a client.
 */
public class ClientSecurityConfig {

    private Credentials credentials;

    private String credentialsClassname;

    public Credentials getCredentials() {
        return credentials;
    }

    public ClientSecurityConfig setCredentials(Credentials credentials) {
        this.credentials = credentials;
        return this;
    }

    public String getCredentialsClassname() {
        return credentialsClassname;
    }

    public ClientSecurityConfig setCredentialsClassname(String credentialsClassname) {
        this.credentialsClassname = credentialsClassname;
        return this;
    }
}
