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

package com.hazelcast.config;

import java.util.Objects;
import java.util.Properties;

import com.hazelcast.config.security.IdentityConfig;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.security.ICredentialsFactory;

/**
 * Contains the configuration for Credentials Factory.
 */
public class CredentialsFactoryConfig implements IdentityConfig {

    private volatile String className;

    private volatile ICredentialsFactory implementation;

    private volatile Properties properties = new Properties();

    public CredentialsFactoryConfig() {
    }

    public CredentialsFactoryConfig(String className) {
        this.className = className;
    }

    private CredentialsFactoryConfig(CredentialsFactoryConfig credentialsFactoryConfig) {
        className = credentialsFactoryConfig.className;
        implementation = credentialsFactoryConfig.implementation;
        properties = new Properties();
        properties.putAll(credentialsFactoryConfig.properties);
    }

    public String getClassName() {
        return className;
    }

    public CredentialsFactoryConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public ICredentialsFactory getImplementation() {
        return implementation;
    }

    public CredentialsFactoryConfig setImplementation(ICredentialsFactory factoryImpl) {
        this.implementation = factoryImpl;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public CredentialsFactoryConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public ICredentialsFactory asCredentialsFactory(ClassLoader cl) {
        if (implementation == null) {
            try {
                implementation = ClassLoaderUtil.newInstance(cl, className);
                implementation.init(properties);
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not create instance of '" + className + "', cause: " + e.getMessage(),
                        e);
            }
        }
        return implementation;
    }

    @Override
    public IdentityConfig copy() {
        return new CredentialsFactoryConfig(this);
    }

    @Override
    public String toString() {
        return "CredentialsFactoryConfig{" + "className='" + className + '\'' + ", implementation=" + implementation
                + ", properties=" + properties + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, implementation, properties);
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
        CredentialsFactoryConfig other = (CredentialsFactoryConfig) obj;
        return Objects.equals(className, other.className) && Objects.equals(implementation, other.implementation)
                && Objects.equals(properties, other.properties);
    }
}
