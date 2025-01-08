/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.hazelcast.internal.util.StringUtil;

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
                return LoginModuleUsage.valueOf(StringUtil.upperCaseInternal(v));
            } catch (Exception ignored) {
            }
            return REQUIRED;
        }
    }

    public String getClassName() {
        return className;
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

    public LoginModuleConfig setUsage(LoginModuleUsage usage) {
        this.usage = usage;
        return this;
    }

    public LoginModuleConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    public LoginModuleConfig setProperty(String key, String value) {
        properties.setProperty(key, value);
        return this;
    }

    /**
     * Alternative to the {@link #setProperty(String, String)} method. This method allows to use {@code null} as {@code value}
     * parameter. In such case is the property removed.
     *
     * @param key property name (must not be {@code null})
     * @param value new value or {@code null}
     * @return this instance
     */
    public LoginModuleConfig setOrClear(@Nonnull String key, @Nullable String value) {
        if (value != null) {
            properties.setProperty(key, value);
        } else {
            properties.remove(key);
        }
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
        if (!(o instanceof LoginModuleConfig that)) {
            return false;
        }
        return Objects.equals(className, that.className)
                && usage == that.usage
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, usage, properties);
    }
}
