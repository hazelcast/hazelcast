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

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.security.ICredentialsFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class configures the Kerberos identity. Based on this configuration, service tickets are retrieved from Kerberos KDC
 * (Key Distribution Center).
 */
public class KerberosIdentityConfig implements IdentityConfig {

    private final CredentialsFactoryConfig factoryConfig = new CredentialsFactoryConfig(
            "com.hazelcast.security.impl.KerberosCredentialsFactory");

    public String getSpn() {
        return factoryConfig.getProperties().getProperty("spn");
    }

    /**
     * Allows to configure static service principal name (SPN). It's meant for usecases where all members share a single
     * Kerberos identity.
     */
    public KerberosIdentityConfig setSpn(String spn) {
        factoryConfig.getProperties().setProperty("spn", spn);
        return this;
    }

    public String getServiceNamePrefix() {
        return factoryConfig.getProperties().getProperty("serviceNamePrefix");
    }

    /**
     * Defines prefix of the Service Principal name. It's default value is {@code "hz/"}. By default the member's principal name
     * (for which this credentials factory asks the service ticket) is in form "[servicePrefix][memberIpAddress]@[REALM]" (e.g.
     * "hz/192.168.1.1@ACME.COM").
     */
    public KerberosIdentityConfig setServiceNamePrefix(String serviceNamePrefix) {
        factoryConfig.getProperties().setProperty("serviceNamePrefix", serviceNamePrefix);
        return this;
    }

    public String getRealm() {
        return factoryConfig.getProperties().getProperty("realm");
    }

    /**
     * Defines Kerberos realm name (e.g. "ACME.COM").
     */
    public KerberosIdentityConfig setRealm(String realm) {
        factoryConfig.getProperties().setProperty("realm", realm);
        return this;
    }

    public String getKeytabFile() {
        return factoryConfig.getProperties().getProperty("keytabFile");
    }

    /**
     * Allows (together with the {@link #setPrincipal(String)}) simplification of security realm configuration. For basic
     * scenarios you don't need to use {@link #setSecurityRealm(String)}, but you can instead define directly kerberos principal
     * name and keytab file path with credentials for given principal.
     * <p>
     * This configuration is only used when there is no {@code securityRealm} configured.
     */
    public KerberosIdentityConfig setKeytabFile(String keytabFile) {
        factoryConfig.getProperties().setProperty("keytabFile", keytabFile);
        return this;
    }

    public String getPrincipal() {
        return factoryConfig.getProperties().getProperty("principal");
    }

    /**
     * Allows (together with the {@link #setKeytabFile(String)}) simplification of security realm configuration. For basic
     * scenarios you don't need to use {@link #setSecurityRealm(String)}, but you can instead define directly kerberos principal
     * name and keytab file path with credentials for given principal.
     * <p>
     * This configuration is only used when there is no {@code securityRealm} configured.
     */
    public KerberosIdentityConfig setPrincipal(String principal) {
        factoryConfig.getProperties().setProperty("principal", principal);
        return this;
    }

    public String getSecurityRealm() {
        return factoryConfig.getProperties().getProperty("securityRealm");
    }

    /**
     * Configures a reference to Security realm name in Hazelcast configuration. The realm's authentication configuration (when
     * defined) is used to populate the user object with Kerberos credentials (e.g. TGT).
     */
    public KerberosIdentityConfig setSecurityRealm(String securityRealm) {
        factoryConfig.getProperties().setProperty("securityRealm", securityRealm);
        return this;
    }

    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "Proper support in the config XML generator.")
    public Boolean getUseCanonicalHostname() {
        String strVal = factoryConfig.getProperties().getProperty("useCanonicalHostname");
        return strVal != null ? Boolean.parseBoolean(strVal) : null;
    }

    /**
     * Allows using fully qualified domain name instead of IP address when the SPN is constructed from a prefix and realm name.
     * For instance, when set {@code true}, the SPN {@code "hz/192.168.1.1@ACME.COM"} could become
     * {@code "hz/member1.acme.com@ACME.COM"} (given the reverse DNS lookup for 192.168.1.1 returns the "member1.acme.com"
     * hostname).
     */
    public KerberosIdentityConfig setUseCanonicalHostname(Boolean useCanonicalHostname) {
        Properties props = factoryConfig.getProperties();
        if (useCanonicalHostname != null) {
            props.setProperty("useCanonicalHostname", useCanonicalHostname.toString());
        } else {
            props.remove("useCanonicalHostname");
        }
        return this;
    }

    @Override
    public ICredentialsFactory asCredentialsFactory(ClassLoader cl) {
        return factoryConfig.asCredentialsFactory(cl);
    }

    @Override
    public IdentityConfig copy() {
        return new KerberosIdentityConfig().setRealm(getRealm()).setSecurityRealm(getSecurityRealm())
                .setServiceNamePrefix(getServiceNamePrefix()).setSpn(getSpn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(factoryConfig);
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
        KerberosIdentityConfig other = (KerberosIdentityConfig) obj;
        return Objects.equals(factoryConfig, other.factoryConfig);
    }

    @Override
    public String toString() {
        return "KerberosIdentityConfig [spn=" + getSpn() + ", serviceNamePrefix=" + getServiceNamePrefix() + ", realm="
                + getRealm() + ", securityRealm=" + getSecurityRealm() + ", principal=" + getPrincipal() + ", keytabFile="
                + getKeytabFile() + ", useCanonicalHostname=" + getUseCanonicalHostname() + "]";
    }

}
