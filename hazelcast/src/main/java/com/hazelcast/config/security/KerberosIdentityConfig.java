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

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.security.ICredentialsFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KerberosIdentityConfig implements IdentityConfig {

    private final CredentialsFactoryConfig factoryConfig = new CredentialsFactoryConfig(
            "com.hazelcast.security.impl.KerberosCredentialsFactory");

    public String getSpn() {
        return factoryConfig.getProperties().getProperty("spn");
    }

    public KerberosIdentityConfig setSpn(String spn) {
        factoryConfig.getProperties().setProperty("spn", spn);
        return this;
    }

    public String getServiceNamePrefix() {
        return factoryConfig.getProperties().getProperty("serviceNamePrefix");
    }

    public KerberosIdentityConfig setServiceNamePrefix(String serviceNamePrefix) {
        factoryConfig.getProperties().setProperty("serviceNamePrefix", serviceNamePrefix);
        return this;
    }

    public String getRealm() {
        return factoryConfig.getProperties().getProperty("realm");
    }

    public KerberosIdentityConfig setRealm(String realm) {
        factoryConfig.getProperties().setProperty("realm", realm);
        return this;
    }

    public String getSecurityRealm() {
        return factoryConfig.getProperties().getProperty("securityRealm");
    }

    public KerberosIdentityConfig setSecurityRealm(String securityRealm) {
        factoryConfig.getProperties().setProperty("securityRealm", securityRealm);
        return this;
    }

    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "Proper support in the config XML generator.")
    public Boolean getUseCanonicalHostname() {
        String strVal = factoryConfig.getProperties().getProperty("useCanonicalHostname");
        return strVal != null ? Boolean.parseBoolean(strVal) : null;
    }

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
        return "KerberosIdentityConfig [spn=" + getSpn() + ", serviceNamePrefix=" + getServiceNamePrefix()
                + ", realm=" + getRealm()
                + ", securityRealm=" + getSecurityRealm()
                + ", useCanonicalHostname=" + getUseCanonicalHostname()
                + "]";
    }

}
