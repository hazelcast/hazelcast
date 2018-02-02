/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.nio.ssl.TlsHostVerifier;

import java.util.Properties;

/**
 * TLS host verification configuration holder.
 */
public class HostVerificationConfig {

    private String policyClassName;
    private TlsHostVerifier implementation;
    private boolean enabledOnServer;
    private Properties properties = new Properties();

    /**
     * Returns a class name implementing {@link com.hazelcast.nio.ssl.TlsHostVerifier} interface.
     * @return TLS host verifier class name
     */
    public String getPolicyClassName() {
        return policyClassName;
    }

    /**
     * Sets a class name implementing {@link com.hazelcast.nio.ssl.TlsHostVerifier} interface.
     * @return this instance
     */
    public HostVerificationConfig setPolicyClassName(String policyClassName) {
        this.policyClassName = policyClassName;
        return this;
    }

    /**
     * Returns if TLS host verification should also proceed on the server side of TLS connection.
     */
    public boolean isEnabledOnServer() {
        return enabledOnServer;
    }

    /**
     * Sets if TLS host verification should also proceed on the server side of TLS connection. By default only the client side
     * is checked.
     * @return this instance
     */
    public HostVerificationConfig setEnabledOnServer(boolean enabledOnServer) {
        this.enabledOnServer = enabledOnServer;
        return this;
    }

    /**
     * Returns TLS host verifier properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets TLS host verifier properties. They are provided to {@link com.hazelcast.nio.ssl.TlsHostVerifier#init(Properties)}
     * method during verifier initialization.
     */
    public HostVerificationConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Adds a single property to {@link Properties} object used for TLS host verifier initialization.
     */
    public HostVerificationConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    /**
     * Returns the implementation object.
     * @return the implementation
     */
    public TlsHostVerifier getImplementation() {
        return implementation;
    }

    /**
     * Sets the implementation object. If the implementation is configured, then it takes precedence over the
     * {@codepolicyClassName}.
     *
     * @param implementation the implementation to set
     * @return this instance
     */
    public HostVerificationConfig setImplementation(TlsHostVerifier implementation) {
        this.implementation = implementation;
        return this;
    }

    @Override
    public String toString() {
        return "HostVerification{policyClassName=" + policyClassName
                + ", implementation=" + implementation
                + ", enabledOnServer=" + enabledOnServer
                + ", properties=" + properties + "}";
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof HostVerificationConfig)) {
            return false;
        }

        HostVerificationConfig that = (HostVerificationConfig) o;

        if (enabledOnServer != that.enabledOnServer) {
            return false;
        }
        if (policyClassName != null ? !policyClassName.equals(that.policyClassName) : that.policyClassName != null) {
            return false;
        }
        if (implementation != null ? !implementation.equals(that.implementation) : that.implementation != null) {
            return false;
        }
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public final int hashCode() {
        int result = policyClassName != null ? policyClassName.hashCode() : 0;
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + (enabledOnServer ? 1 : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
