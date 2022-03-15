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
import java.util.Properties;

import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;

/**
 * Typed authentication configuration for the {@code X509CertificateLoginModule}.
 */
public class TlsAuthenticationConfig extends AbstractClusterLoginConfig<TlsAuthenticationConfig> {

    private String roleAttribute;

    public String getRoleAttribute() {
        return roleAttribute;
    }

    public TlsAuthenticationConfig setRoleAttribute(String roleAttribute) {
        this.roleAttribute = roleAttribute;
        return this;
    }

    @Override
    protected Properties initLoginModuleProperties() {
        Properties props = super.initLoginModuleProperties();
        setIfConfigured(props, "roleAttribute", roleAttribute);
        return props;
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig("com.hazelcast.security.loginimpl.X509CertificateLoginModule",
                LoginModuleUsage.REQUIRED);
        loginModuleConfig.setProperties(initLoginModuleProperties());
        return new LoginModuleConfig[] { loginModuleConfig };
    }

    @Override
    public String toString() {
        return "TlsAuthenticationConfig [roleAttribute=" + roleAttribute + ", getSkipIdentity()=" + getSkipIdentity()
                + ", getSkipEndpoint()=" + getSkipEndpoint() + ", getSkipRole()=" + getSkipRole() + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(roleAttribute);
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
        TlsAuthenticationConfig other = (TlsAuthenticationConfig) obj;
        return Objects.equals(roleAttribute, other.roleAttribute);
    }

    @Override
    protected TlsAuthenticationConfig self() {
        return this;
    }
}
