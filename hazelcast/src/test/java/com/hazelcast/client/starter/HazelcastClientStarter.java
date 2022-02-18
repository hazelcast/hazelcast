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

package com.hazelcast.client.starter;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.HazelcastStarter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.HazelcastStarter.getConfig;
import static com.hazelcast.test.starter.HazelcastStarter.getTargetVersionClassloader;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static java.lang.Thread.currentThread;

public class HazelcastClientStarter {

    private HazelcastClientStarter() {
    }

    public static HazelcastInstance newHazelcastClient(String version, boolean enterprise) {
        return newHazelcastClient(version, null, enterprise);
    }

    @SuppressWarnings("unchecked")
    public static HazelcastInstance newHazelcastClient(String version, ClientConfig clientConfig, boolean enterprise) {
        return newHazelcastClient(version, clientConfig, enterprise, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    public static HazelcastInstance newHazelcastClient(String version, ClientConfig clientConfig, boolean enterprise, List<URL> additionalJars) {
        ClassLoader classLoader = clientConfig == null ? null : clientConfig.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = getTargetVersionClassloader(version, enterprise,
                classLoader, additionalJars);
        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            Class<HazelcastClient> hazelcastClass
                    = (Class<HazelcastClient>) classloader.loadClass("com.hazelcast.client.HazelcastClient");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.client.config.ClientConfig");
            Object config = getConfig(classloader, configClass, clientConfig);

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastClient", configClass);
            Object delegate = newHazelcastInstanceMethod.invoke(null, config);
            return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);
        } catch (ClassNotFoundException e) {
            throw rethrowGuardianException(e);
        } catch (NoSuchMethodException e) {
            throw rethrowGuardianException(e);
        } catch (IllegalAccessException e) {
            throw rethrowGuardianException(e);
        } catch (InvocationTargetException e) {
            throw rethrowGuardianException(e);
        } catch (InstantiationException e) {
            throw rethrowGuardianException(e);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }
}
