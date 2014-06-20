/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.util.EmptyStatement;

import java.util.Properties;
/**
 * Configuration for Login Module
 */

public class LoginModuleConfig {

    private String className;
    private LoginModuleUsage usage;
    private Properties properties = new Properties();

    public LoginModuleConfig() {
        super();
    }

    public LoginModuleConfig(String className, LoginModuleUsage usage) {
        super();
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
                return LoginModuleUsage.valueOf(v.toUpperCase());
            } catch (Exception ignore) {
                EmptyStatement.ignore(ignore);
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("LoginModuleConfig");
        sb.append("{className='").append(className).append('\'');
        sb.append(", usage=").append(usage);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
