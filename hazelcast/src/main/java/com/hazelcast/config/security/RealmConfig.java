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

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;

/**
 * Security realm represents the security configuration for part of the system (e.g. member-to-member comunication).
 */
public class RealmConfig {

    /**
     * Default security realm instance.
     */
    public static final RealmConfig DEFAULT_REALM = new RealmConfig();

    private AuthenticationConfig authenticationConfig = DefaultAuthenticationConfig.INSTANCE;
    private IdentityConfig identityConfig;

    public JaasAuthenticationConfig getJaasAuthenticationConfig() {
        return getIfType(authenticationConfig, JaasAuthenticationConfig.class);
    }

    public RealmConfig setJaasAuthenticationConfig(JaasAuthenticationConfig authenticationConfig) {
        this.authenticationConfig = requireNonNull(authenticationConfig, "Authentication config can't be null");
        return this;
    }

    public TlsAuthenticationConfig getTlsAuthenticationConfig() {
        return getIfType(authenticationConfig, TlsAuthenticationConfig.class);
    }

    public RealmConfig setTlsAuthenticationConfig(TlsAuthenticationConfig authenticationConfig) {
        this.authenticationConfig = requireNonNull(authenticationConfig, "Authentication config can't be null");
        return this;
    }

    public LdapAuthenticationConfig getLdapAuthenticationConfig() {
        return getIfType(authenticationConfig, LdapAuthenticationConfig.class);
    }

    public RealmConfig setLdapAuthenticationConfig(LdapAuthenticationConfig authenticationConfig) {
        this.authenticationConfig = requireNonNull(authenticationConfig, "Authentication config can't be null");
        return this;
    }

    public KerberosAuthenticationConfig getKerberosAuthenticationConfig() {
        return getIfType(authenticationConfig, KerberosAuthenticationConfig.class);
    }

    public RealmConfig setKerberosAuthenticationConfig(KerberosAuthenticationConfig authenticationConfig) {
        this.authenticationConfig = requireNonNull(authenticationConfig, "Authentication config can't be null");
        return this;
    }

    public SimpleAuthenticationConfig getSimpleAuthenticationConfig() {
        return getIfType(authenticationConfig, SimpleAuthenticationConfig.class);
    }

    public RealmConfig setSimpleAuthenticationConfig(SimpleAuthenticationConfig authenticationConfig) {
        this.authenticationConfig = requireNonNull(authenticationConfig, "Authentication config can't be null");
        return this;
    }

    public UsernamePasswordIdentityConfig getUsernamePasswordIdentityConfig() {
        return getIfType(identityConfig, UsernamePasswordIdentityConfig.class);
    }

    public RealmConfig setUsernamePasswordIdentityConfig(UsernamePasswordIdentityConfig identityConfig) {
        this.identityConfig = identityConfig;
        return this;
    }

    public RealmConfig setUsernamePasswordIdentityConfig(String username, String password) {
        this.identityConfig = new UsernamePasswordIdentityConfig(username, password);
        return this;
    }

    public TokenIdentityConfig getTokenIdentityConfig() {
        return getIfType(identityConfig, TokenIdentityConfig.class);
    }

    public RealmConfig setTokenIdentityConfig(TokenIdentityConfig identityConfig) {
        this.identityConfig = identityConfig;
        return this;
    }

    public CredentialsFactoryConfig getCredentialsFactoryConfig() {
        return getIfType(identityConfig, CredentialsFactoryConfig.class);
    }

    public RealmConfig setCredentialsFactoryConfig(CredentialsFactoryConfig identityConfig) {
        this.identityConfig = identityConfig;
        return this;
    }

    public CredentialsIdentityConfig getCredentialsIdentityConfig() {
        return getIfType(identityConfig, CredentialsIdentityConfig.class);
    }

    public RealmConfig setCredentialsIdentityConfig(CredentialsIdentityConfig identity) {
        this.identityConfig = identity;
        return this;
    }

    public RealmConfig setCredentials(Credentials credentials) {
        this.identityConfig = new CredentialsIdentityConfig(credentials);
        return this;
    }

    public KerberosIdentityConfig getKerberosIdentityConfig() {
        return getIfType(identityConfig, KerberosIdentityConfig.class);
    }

    public RealmConfig setKerberosIdentityConfig(KerberosIdentityConfig identityConfig) {
        this.identityConfig = identityConfig;
        return this;
    }

    public boolean isAuthenticationConfigured() {
        return authenticationConfig != null && authenticationConfig != DefaultAuthenticationConfig.INSTANCE;
    }

    public boolean isIdentityConfigured() {
        return identityConfig != null;
    }

    public LoginModuleConfig[] asLoginModuleConfigs() {
        if (authenticationConfig == null) {
            return null;
        }
        return authenticationConfig.asLoginModuleConfigs();
    }

    public ICredentialsFactory asCredentialsFactory() {
        return identityConfig != null ? identityConfig.asCredentialsFactory(null) : null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticationConfig, identityConfig);
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
        RealmConfig other = (RealmConfig) obj;
        return Objects.equals(authenticationConfig, other.authenticationConfig)
                && Objects.equals(identityConfig, other.identityConfig);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RealmConfig [authenticationConfig=").append(authenticationConfig).append(", identityConfig=")
                .append(identityConfig).append("]");
        return builder.toString();
    }

    private <T> T getIfType(Object inst, Class<T> clazz) {
        return clazz.isInstance(inst) ? clazz.cast(inst) : null;
    }
}
