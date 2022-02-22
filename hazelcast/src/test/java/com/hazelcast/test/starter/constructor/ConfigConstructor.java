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

package com.hazelcast.test.starter.constructor;

import com.hazelcast.test.starter.HazelcastStarterConstructor;

import java.lang.reflect.Method;

/**
 * Clones the configuration from {@code mainConfig} to a new configuration object loaded in the
 * target {@code classloader}. The returned configuration has its classloader set to the target classloader.
 */
@HazelcastStarterConstructor(classNames = {"com.hazelcast.config.Config", "com.hazelcast.client.config.ClientConfig"})
public class ConfigConstructor extends AbstractConfigConstructor {

    public ConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Object otherConfig = cloneConfig(delegate, classloader);

        Method setClassLoaderMethod = targetClass.getMethod("setClassLoader", ClassLoader.class);
        setClassLoaderMethod.invoke(otherConfig, classloader);
        return otherConfig;
    }
}
