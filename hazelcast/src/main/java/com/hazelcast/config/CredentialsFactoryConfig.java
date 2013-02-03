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

import com.hazelcast.security.ICredentialsFactory;

import java.util.Properties;

public class CredentialsFactoryConfig {

    private String className = null;

    private ICredentialsFactory implementation = null;

    private Properties properties = new Properties();

    public CredentialsFactoryConfig() {
        super();
    }

    public CredentialsFactoryConfig(String className) {
        super();
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public ICredentialsFactory getImplementation() {
        return implementation;
    }

    public void setImplementation(ICredentialsFactory factoryImpl) {
        this.implementation = factoryImpl;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("CredentialsFactoryConfig");
        sb.append("{className='").append(className).append('\'');
        sb.append(", implementation=").append(implementation);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
