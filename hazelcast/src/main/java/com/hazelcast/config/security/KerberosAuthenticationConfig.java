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
 * Typed authentication configuration for Kerberos tickets verification.
 */
public class KerberosAuthenticationConfig extends AbstractClusterLoginConfig<KerberosAuthenticationConfig> {

    private Boolean relaxFlagsCheck;
    private Boolean useNameWithoutRealm;
    private String securityRealm;
    private String keytabFile;
    private String principal;
    private LdapAuthenticationConfig ldapAuthenticationConfig;

    public Boolean getRelaxFlagsCheck() {
        return relaxFlagsCheck;
    }

    /**
     * Allows disabling some of the checks on incoming token (e.g. passes authentication even if the mutual authentication is
     * required by the token).
     */
    public KerberosAuthenticationConfig setRelaxFlagsCheck(Boolean relaxFlagsCheck) {
        this.relaxFlagsCheck = relaxFlagsCheck;
        return this;
    }

    public String getSecurityRealm() {
        return securityRealm;
    }

    /**
     * Allows cutting off the Kerberos realm part from authenticated name. When set to {@code true}, the {@code '@REALM'} part
     * is removed from the name (e.g. {@code jduke@ACME.COM} becomes {@code jduke}).
     */
    public KerberosAuthenticationConfig setUseNameWithoutRealm(Boolean useNameWithoutRealm) {
        this.useNameWithoutRealm = useNameWithoutRealm;
        return this;
    }

    public Boolean getUseNameWithoutRealm() {
        return useNameWithoutRealm;
    }

    /**
     * References Security realm name in Hazelcast configuration. The realm's authentication configuration (when defined) will
     * be used to fill the user object with Kerberos credentials (e.g. KeyTab entry).
     */
    public KerberosAuthenticationConfig setSecurityRealm(String securityRealm) {
        this.securityRealm = securityRealm;
        return this;
    }

    public LdapAuthenticationConfig getLdapAuthenticationConfig() {
        return ldapAuthenticationConfig;
    }

    /**
     * Allows specifying LDAP authentication configuration which is then used after the Kerberos authentication successfully
     * finishes.
     */
    public KerberosAuthenticationConfig setLdapAuthenticationConfig(LdapAuthenticationConfig ldapAuthenticationConfig) {
        this.ldapAuthenticationConfig = ldapAuthenticationConfig;
        return this;
    }

    public String getKeytabFile() {
        return keytabFile;
    }

    /**
     * Allows (together with the {@link #setPrincipal(String)}) simplification of security realm configuration. For basic
     * scenarios you don't need to use {@link #setSecurityRealm(String)}, but you can instead define directly kerberos principal
     * name and keytab file path with credentials for given principal.
     * <p>
     * This configuration is only used when there is no {@code securityRealm} configured.
     */
    public KerberosAuthenticationConfig setKeytabFile(String keytabFile) {
        this.keytabFile = keytabFile;
        return this;
    }

    public String getPrincipal() {
        return principal;
    }

    /**
     * Allows (together with the {@link #setKeytabFile(String)}) simplification of security realm configuration. For basic
     * scenarios you don't need to use {@link #setSecurityRealm(String)}, but you can instead define directly kerberos principal
     * name and keytab file path with credentials for given principal.
     * <p>
     * This configuration is only used when there is no {@code securityRealm} configured.
     */
    public KerberosAuthenticationConfig setPrincipal(String principal) {
        this.principal = principal;
        return this;
    }

    @Override
    protected Properties initLoginModuleProperties() {
        Properties props = super.initLoginModuleProperties();
        setIfConfigured(props, "relaxFlagsCheck", relaxFlagsCheck);
        setIfConfigured(props, "useNameWithoutRealm", useNameWithoutRealm);
        setIfConfigured(props, "securityRealm", securityRealm);
        setIfConfigured(props, "keytabFile", keytabFile);
        setIfConfigured(props, "principal", principal);
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
        result = prime * result + Objects.hash(ldapAuthenticationConfig, relaxFlagsCheck, useNameWithoutRealm, securityRealm,
                keytabFile, principal);
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
                && Objects.equals(relaxFlagsCheck, other.relaxFlagsCheck)
                && Objects.equals(useNameWithoutRealm, other.useNameWithoutRealm)
                && Objects.equals(keytabFile, other.keytabFile) && Objects.equals(principal, other.principal)
                && Objects.equals(securityRealm, other.securityRealm);
    }

    @Override
    public String toString() {
        return "KerberosAuthenticationConfig [relaxFlagsCheck=" + relaxFlagsCheck + ", securityRealm=" + securityRealm
                + ", useNameWithoutRealm=" + useNameWithoutRealm + ", ldapAuthenticationConfig=" + ldapAuthenticationConfig
                + ", keytabFile=" + keytabFile + ", principal=" + principal + ", getSkipIdentity()=" + getSkipIdentity()
                + ", getSkipEndpoint()=" + getSkipEndpoint() + ", getSkipRole()=" + getSkipRole() + "]";
    }

    @Override
    protected KerberosAuthenticationConfig self() {
        return this;
    }
}
