/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.security.Credentials;

/**
 * Contains the security configuration for the client.
 * Credentials object is used for both authentication and authorization
 * Credentials is used with ClusterLoginModule for authentication
 * It is also used with SecurityInterceptor and to define principal in client-permissions for authorization.
 *
 * @see Credentials#getPrincipal()
 * @see com.hazelcast.security.SecurityInterceptor
 */
public class ClientSecurityConfig {

    private Credentials credentials;
    private String credentialsClassname;
    private CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig();

    public ClientSecurityConfig() {
    }

    public ClientSecurityConfig(ClientSecurityConfig securityConfig) {
        credentials = securityConfig.credentials;
        credentialsClassname = securityConfig.credentialsClassname;
        credentialsFactoryConfig = new CredentialsFactoryConfig(securityConfig.credentialsFactoryConfig);
    }

    public Credentials getCredentials() {
        return credentials;
    }

    /**
     * @param credentials that will be used when
     * @return configured {@link com.hazelcast.client.config.ClientSecurityConfig} for chaining
     */
    public ClientSecurityConfig setCredentials(Credentials credentials) {
        this.credentials = credentials;
        return this;
    }

    /**
     * @return configured class name for credentials
     */
    public String getCredentialsClassname() {
        return credentialsClassname;
    }

    /**
     * Credentials class will be instantiated from class name when setCredentialsFactoryConfig and  setCredentials
     * are not used. The class will be instantiated with empty constructor.
     *
     * @param credentialsClassname class name for credentials
     * @return configured {@link com.hazelcast.client.config.ClientSecurityConfig} for chaining
     */
    public ClientSecurityConfig setCredentialsClassname(String credentialsClassname) {
        this.credentialsClassname = credentialsClassname;
        return this;
    }

    /**
     * @return credentials factory config
     */
    public CredentialsFactoryConfig getCredentialsFactoryConfig() {
        return credentialsFactoryConfig;
    }

    /**
     * Credentials Factory Config allows user to pass custom properties and use group config when instantiating
     * a credentials object.
     *
     * @param credentialsFactoryConfig the config that will be used to create credentials factory
     * @return configured {@link com.hazelcast.client.config.ClientSecurityConfig} for chaining
     */
    public ClientSecurityConfig setCredentialsFactoryConfig(CredentialsFactoryConfig credentialsFactoryConfig) {
        this.credentialsFactoryConfig = credentialsFactoryConfig;
        return this;
    }

    @Override
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClientSecurityConfig that = (ClientSecurityConfig) o;

        if (credentials != null ? !credentials.equals(that.credentials) : that.credentials != null) {
            return false;
        }
        if (credentialsClassname != null
                ? !credentialsClassname.equals(that.credentialsClassname) : that.credentialsClassname != null) {
            return false;
        }
        return credentialsFactoryConfig != null
                ? credentialsFactoryConfig.equals(that.credentialsFactoryConfig) : that.credentialsFactoryConfig == null;
    }

    @Override
    public int hashCode() {
        int result = credentials != null ? credentials.hashCode() : 0;
        result = 31 * result + (credentialsClassname != null ? credentialsClassname.hashCode() : 0);
        result = 31 * result + (credentialsFactoryConfig != null ? credentialsFactoryConfig.hashCode() : 0);
        return result;
    }
}
