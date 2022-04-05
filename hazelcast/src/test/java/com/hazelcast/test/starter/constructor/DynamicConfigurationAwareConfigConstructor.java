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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;

@HazelcastStarterConstructor(classNames = {"com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig"})
public class DynamicConfigurationAwareConfigConstructor extends AbstractConfigConstructor {

    public DynamicConfigurationAwareConfigConstructor(Class<?> targetClass) {
        super(targetClass);
    }

    @Override
    Object createNew0(Object delegate) throws Exception {
        ClassLoader classloader = targetClass.getClassLoader();
        Class<?> configClass = classloader.loadClass("com.hazelcast.config.Config");
        Class<?> propertiesClass = classloader.loadClass("com.hazelcast.spi.properties.HazelcastProperties");
        Constructor<?> constructor = targetClass.getDeclaredConstructor(configClass, propertiesClass);

        Object config = getFieldValueReflectively(delegate, "staticConfig");
        Object clonedConfig = cloneConfig(config, classloader);

        Constructor<?> propertiesConstructor = propertiesClass.getDeclaredConstructor(configClass);
        Object clonedHazelcastProperties = propertiesConstructor.newInstance(clonedConfig);

        Method setClassLoaderMethod = configClass.getMethod("setClassLoader", ClassLoader.class);
        setClassLoaderMethod.invoke(clonedConfig, classloader);

        Object[] args = new Object[]{clonedConfig, clonedHazelcastProperties};
        return constructor.newInstance(args);
    }
}
