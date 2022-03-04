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

import com.hazelcast.security.IPermissionPolicy;

import java.util.Properties;
/**
 * Contains the configuration for policy of permission
 */
public class PermissionPolicyConfig {

    private String className;

    private IPermissionPolicy implementation;

    private Properties properties = new Properties();

    public PermissionPolicyConfig() {
    }

    public PermissionPolicyConfig(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    public PermissionPolicyConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public IPermissionPolicy getImplementation() {
        return implementation;
    }

    public PermissionPolicyConfig setImplementation(IPermissionPolicy policyImpl) {
        this.implementation = policyImpl;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public PermissionPolicyConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        return "PermissionPolicyConfig{"
                + "className='" + className + '\''
                + ", implementation=" + implementation
                + ", properties=" + properties
                + '}';
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PermissionPolicyConfig that = (PermissionPolicyConfig) o;

        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (implementation != null ? !implementation.equals(that.implementation) : that.implementation != null) {
            return false;
        }
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
