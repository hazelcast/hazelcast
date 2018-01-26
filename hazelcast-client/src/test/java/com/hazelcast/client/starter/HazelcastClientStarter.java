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
package com.hazelcast.client.starter;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.HazelcastStarter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static com.hazelcast.test.starter.HazelcastProxyFactory.proxyObjectForStarter;
import static com.hazelcast.test.starter.HazelcastStarter.getConfig;
import static com.hazelcast.test.starter.HazelcastStarter.getTargetVersionClassloader;
import static com.hazelcast.test.starter.Utils.rethrow;
import static java.lang.Thread.currentThread;

public class HazelcastClientStarter {

    private HazelcastClientStarter() {
    }

    public static HazelcastInstance newHazelcastClient(String version, boolean enterprise) {
        return newHazelcastClient(version, null, enterprise);
    }

    public static HazelcastInstance newHazelcastClient(String version, ClientConfig clientConfig, boolean enterprise) {
        ClassLoader classLoader = clientConfig == null ? null : clientConfig.getClassLoader();
        HazelcastAPIDelegatingClassloader classloader = getTargetVersionClassloader(version, enterprise, classLoader);
        ClassLoader contextClassLoader = currentThread().getContextClassLoader();
        currentThread().setContextClassLoader(null);
        try {
            Class<Hazelcast> hazelcastClass = (Class<Hazelcast>) classloader.loadClass("com.hazelcast.client.HazelcastClient");
            System.out.println(hazelcastClass + " loaded by " + hazelcastClass.getClassLoader());
            Class<?> configClass = classloader.loadClass("com.hazelcast.client.config.ClientConfig");
            Object config = getConfig(clientConfig, classloader, configClass);

            Method newHazelcastInstanceMethod = hazelcastClass.getMethod("newHazelcastClient", configClass);
            Object delegate = newHazelcastInstanceMethod.invoke(null, config);
            return (HazelcastInstance) proxyObjectForStarter(HazelcastStarter.class.getClassLoader(), delegate);

        } catch (ClassNotFoundException e) {
            throw rethrow(e);
        } catch (NoSuchMethodException e) {
            throw rethrow(e);
        } catch (IllegalAccessException e) {
            throw rethrow(e);
        } catch (InvocationTargetException e) {
            throw rethrow(e);
        } catch (InstantiationException e) {
            throw rethrow(e);
        } finally {
            if (contextClassLoader != null) {
                currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

}
