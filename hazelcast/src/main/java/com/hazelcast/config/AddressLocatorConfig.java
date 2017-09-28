/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.AddressLocator;

import java.util.Properties;

import static com.hazelcast.util.Preconditions.checkNotNull;


/**
 * Configuration for a custom Address Locator strategy.
 *
 * Address locator allows to plug in own strategy to customize:
 * 1. Address Hazelcast is binding to
 * 2. Address Hazelcast will advertise to others
 *
 * In most environments you don't need to customize this and the default strategy will work just
 * fine. However in some cloud environments the default strategy does not make the right choice and
 * address locator delegates the process of address picking to external code.
 */
public final class AddressLocatorConfig {
    private boolean enabled;
    private String classname;
    private Properties properties = new Properties();
    private AddressLocator implementation;


    public boolean isEnabled() {
        return enabled;
    }

    public AddressLocatorConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassname() {
        return classname;
    }

    public AddressLocatorConfig setClassname(String classname) {
        this.classname = classname;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public AddressLocatorConfig setProperties(Properties properties) {
        checkNotNull(properties, "AddressLocator properties cannot be null");
        this.properties = properties;
        return this;
    }

    public AddressLocator getImplementation() {
        return implementation;
    }

    public AddressLocatorConfig setImplementation(AddressLocator implementation) {
        this.implementation = implementation;
        return this;
    }

    @Override
    public String toString() {
        return "AddressLocatorConfig{"
                + "enabled=" + enabled
                + ", classname='" + classname + '\''
                + ", properties=" + properties
                + ", implementation=" + implementation
                + '}';
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AddressLocatorConfig that = (AddressLocatorConfig) o;

        if (isEnabled() != that.isEnabled()) {
            return false;
        }
        if (getClassname() != null ? !getClassname().equals(that.getClassname()) : that.getClassname() != null) {
            return false;
        }
        if (!getProperties().equals(that.getProperties())) {
            return false;
        }
        return getImplementation() != null ? getImplementation().equals(that.getImplementation())
                : that.getImplementation() == null;
    }

    @Override
    public int hashCode() {
        int result = (isEnabled() ? 1 : 0);
        result = 31 * result + (getClassname() != null ? getClassname().hashCode() : 0);
        result = 31 * result + getProperties().hashCode();
        result = 31 * result + (getImplementation() != null ? getImplementation().hashCode() : 0);
        return result;
    }
}
