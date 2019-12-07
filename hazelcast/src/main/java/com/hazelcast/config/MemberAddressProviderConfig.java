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

package com.hazelcast.config;

import com.hazelcast.spi.MemberAddressProvider;

import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;


/**
 * Configuration for a custom {@link MemberAddressProvider} strategy.
 * <p>
 * The member address provider allows you to plug your own strategy to customize:
 * <ul>
 * <li>What address Hazelcast will bind to</li>
 * <li>What address Hazelcast will advertise to other members on which they can bind to</li>
 * </ul>
 * In most environments you don't need to customize this and the default strategy will work just
 * fine. However in some cloud environments the default strategy does not make the right choice and
 * the member address provider delegates the process of address picking to external code.
 */
public final class MemberAddressProviderConfig {
    private boolean enabled;
    private String className;
    private Properties properties = new Properties();
    private MemberAddressProvider implementation;


    public boolean isEnabled() {
        return enabled;
    }

    public MemberAddressProviderConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public MemberAddressProviderConfig setClassName(String className) {
        if (className != null) {
            setImplementation(null);
        }
        this.className = className;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public MemberAddressProviderConfig setProperties(Properties properties) {
        checkNotNull(properties, "MemberAddressProvider properties cannot be null");
        this.properties = properties;
        return this;
    }

    public MemberAddressProvider getImplementation() {
        return implementation;
    }

    public MemberAddressProviderConfig setImplementation(MemberAddressProvider implementation) {
        if (implementation != null) {
            setClassName(null);
        }
        this.implementation = implementation;
        return this;
    }

    @Override
    public String toString() {
        return "MemberAddressProviderConfig{"
                + "enabled=" + enabled
                + ", className='" + className + '\''
                + ", properties=" + properties
                + ", implementation=" + implementation
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MemberAddressProviderConfig that = (MemberAddressProviderConfig) o;

        return isEnabled() == that.isEnabled()
            && getProperties().equals(that.getProperties())
            && Objects.equals(implementationNameInternal(), that.implementationNameInternal());
    }

    private String implementationNameInternal() {
        if (implementation != null) {
            return implementation.getClass().getName();
        }
        return className;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isEnabled(), implementationNameInternal(), getProperties());
    }
}
