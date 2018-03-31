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

import com.hazelcast.util.StringUtil;

import java.util.Properties;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Configuration for Login Module
 */

public class LoginModuleConfig {

    private String className;
    private LoginModuleUsage usage;
    private Properties properties = new Properties();

    public LoginModuleConfig() {
    }

    public LoginModuleConfig(String className, LoginModuleUsage usage) {
        this.className = className;
        this.usage = usage;
    }

    /**
     * Usage of Login Module
     */
    public enum LoginModuleUsage {
        /**
         * Required
         */
        REQUIRED,
        /**
         * Requisite
         */
        REQUISITE,
        /**
         * Sufficient
         */
        SUFFICIENT,
        /**
         * Optimal
         */
        OPTIONAL;

        public static LoginModuleUsage get(String v) {
            try {
                return LoginModuleUsage.valueOf(v.toUpperCase(StringUtil.LOCALE_INTERNAL));
            } catch (Exception ignore) {
                ignore(ignore);
            }
            return REQUIRED;
        }
    }

    public String getClassName() {
        return className;
    }

    @Deprecated
    /**
     * @deprecated Not supported, to be removed in 4.0. Use {@link #getClassName()} instead
     * @since 3.9
     */
    public Object getImplementation() {
        throw new UnsupportedOperationException("Deprecated operation. Use getClassName instead.");
    }

    public Properties getProperties() {
        return properties;
    }

    public LoginModuleUsage getUsage() {
        return usage;
    }

    public LoginModuleConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    @Deprecated
    /**
     * @deprecated Not supported, to be removed in 4.0. User {@link #setClassName(String)} instead
     * @since 3.9
     */
    public LoginModuleConfig setImplementation(Object implementation) {
        throw new UnsupportedOperationException("Deprecated operation. Use setClassName instead.");
    }

    public LoginModuleConfig setUsage(LoginModuleUsage usage) {
        this.usage = usage;
        return this;
    }

    public LoginModuleConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        return "LoginModuleConfig{className='" + className + "', usage=" + usage
                + ", properties=" + properties + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LoginModuleConfig that = (LoginModuleConfig) o;

        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (usage != that.usage) {
            return false;
        }
        return properties != null ? properties.equals(that.properties) : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (usage != null ? usage.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
