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

package com.hazelcast.test.starter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

public class DynamicConfigurationAwareConfigConstructor extends ConfigConstructor {

    public DynamicConfigurationAwareConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
        Constructor<?> constructor = targetClass.getDeclaredConstructor(configClass);

        Object config = getFieldValueReflectively(delegate, "staticConfig");
        Object clonedConfig = cloneConfig(config, classloader);

        Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
        setClassLoaderMethod.invoke(clonedConfig, classloader);

        Object[] args = new Object[]{clonedConfig};
        return constructor.newInstance(args);
    }
}
