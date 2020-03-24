/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

public class KerberosAuthenticationConfig extends AbstractClusterLoginConfig<KerberosAuthenticationConfig> {

    private Boolean relaxFlagsCheck;
    private String securityRealm;
    private LdapAuthenticationConfig ldapAuthenticationConfig;

    public Boolean getRelaxFlagsCheck() {
        return relaxFlagsCheck;
    }

    public KerberosAuthenticationConfig setRelaxFlagsCheck(Boolean relaxFlagsCheck) {
        this.relaxFlagsCheck = relaxFlagsCheck;
        return this;
    }

    public String getSecurityRealm() {
        return securityRealm;
    }

    public KerberosAuthenticationConfig setSecurityRealm(String securityRealm) {
        this.securityRealm = securityRealm;
        return this;
    }

    public LdapAuthenticationConfig getLdapAuthenticationConfig() {
        return ldapAuthenticationConfig;
    }

    public KerberosAuthenticationConfig setLdapAuthenticationConfig(LdapAuthenticationConfig ldapAuthenticationConfig) {
        this.ldapAuthenticationConfig = ldapAuthenticationConfig;
        return this;
    }

    @Override
    protected Properties initLoginModuleProperties() {
        Properties props = super.initLoginModuleProperties();
        setIfConfigured(props, "relaxFlagsCheck", relaxFlagsCheck);
        setIfConfigured(props, "securityRealm", securityRealm);
        return props;
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig("com.hazelcast.security.loginimpl.GssApiLoginModule",
                LoginModuleUsage.REQUIRED);

        loginModuleConfig.setProperties(initLoginModuleProperties());

        LoginModuleConfig[] loginModuleConfigs = null;
        if (ldapAuthenticationConfig != null) {
            loginModuleConfigs = new LoginModuleConfig[2];
            loginModuleConfigs[1] = ldapAuthenticationConfig.asLoginModuleConfigs()[0];
        } else {
            loginModuleConfigs = new LoginModuleConfig[1];
        }
        loginModuleConfigs[0] = loginModuleConfig;
        return loginModuleConfigs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(ldapAuthenticationConfig, relaxFlagsCheck, securityRealm);
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
        KerberosAuthenticationConfig other = (KerberosAuthenticationConfig) obj;
        return Objects.equals(ldapAuthenticationConfig, other.ldapAuthenticationConfig)
                && Objects.equals(relaxFlagsCheck, other.relaxFlagsCheck) && Objects.equals(securityRealm, other.securityRealm);
    }

    @Override
    public String toString() {
        return "KerberosAuthenticationConfig [relaxFlagsCheck=" + relaxFlagsCheck + ", securityRealm=" + securityRealm
                + ", ldapAuthenticationConfig=" + ldapAuthenticationConfig
                + ", getSkipIdentity()=" + getSkipIdentity() + ", getSkipEndpoint()=" + getSkipEndpoint() + ", getSkipRole()="
                + getSkipRole() + "]";
    }

    @Override
    protected KerberosAuthenticationConfig self() {
        return this;
    }
}
