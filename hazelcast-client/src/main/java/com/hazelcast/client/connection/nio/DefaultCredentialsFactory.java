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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.config.ClientSecurityConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.util.Properties;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * This is the factory that uses Credentials instance giving by either class name or implementation
 * instead of factory given bey user.
 */
public class DefaultCredentialsFactory implements ICredentialsFactory {

    private final Credentials credentials;

    public DefaultCredentialsFactory(ClientSecurityConfig securityConfig, GroupConfig groupConfig,
                                     ClassLoader classLoader) {
        credentials = initCredentials(securityConfig, groupConfig, classLoader);
    }

    private Credentials initCredentials(ClientSecurityConfig securityConfig, GroupConfig groupConfig, ClassLoader classLoader) {
        Credentials credentials = securityConfig.getCredentials();
        if (credentials == null) {
            String credentialsClassname = securityConfig.getCredentialsClassname();
            if (credentialsClassname != null) {
                try {
                    credentials = ClassLoaderUtil.newInstance(classLoader, credentialsClassname);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        }
        if (credentials == null) {
            credentials = new UsernamePasswordCredentials(groupConfig.getName(), groupConfig.getPassword());
        }
        return credentials;
    }

    @Override
    public void configure(GroupConfig groupConfig, Properties properties) {

    }

    @Override
    public Credentials newCredentials() {
        return credentials;
    }

    @Override
    public void destroy() {

    }
}
