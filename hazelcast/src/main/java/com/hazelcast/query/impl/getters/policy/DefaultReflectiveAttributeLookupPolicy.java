/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.getters.policy;

import com.hazelcast.config.AbstractConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.console.SimulateLoadTask;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.config.override.ExternalConfigurationOverride;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.impl.NodeEngine;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Set;

/**
 * RestrictedReflectiveAttributeLookupPolicy is an implementation of the
 * {@link ReflectiveAttributeLookupPolicy} interface that enforces strict rules
 * for reflective attribute lookups.
 */
public final class DefaultReflectiveAttributeLookupPolicy
        implements ReflectiveAttributeLookupPolicy {
    public static final DefaultReflectiveAttributeLookupPolicy INSTANCE = new DefaultReflectiveAttributeLookupPolicy();

    private static final Set<Class<?>> BLOCKED_CLASSES = Set.of(HazelcastInstance.class, Node.class, NodeEngine.class,
            SerializationService.class, NodeExtension.class, SecurityContext.class, Class.class, File.class,
            getNonPublicClass("com.hazelcast.kubernetes.KubernetesTokenProvider"), ExternalConfigurationOverride.class,
            javax.sql.DataSource.class, SimulateLoadTask.class, java.sql.Connection.class, Config.class,
            AbstractConfigBuilder.class, InputStream.class, URL.class);

    private DefaultReflectiveAttributeLookupPolicy() {
    }

    @Override
    public Class<?> verifyClass(Class<?> clazz) throws ReflectiveAttributeLookupException {
        for (Class<?> blockedClass : BLOCKED_CLASSES) {
            if (blockedClass.isAssignableFrom(clazz)) {
                throw new ReflectiveAttributeLookupException("Class " + clazz.getName()
                        + " cannot be used for attribute extraction");
            }
        }
        return clazz;
    }

    @Override
    public Method verifyMethod(Class<?> clazz, Method method) throws ReflectiveAttributeLookupException {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new ReflectiveAttributeLookupException("Method " + clazz.getName() + "#" + method.getName()
                    + " is static and cannot be used for attribute extraction");
        }
        if (method.getReturnType() == Void.TYPE) {
            throw new ReflectiveAttributeLookupException("Method " + clazz.getName() + "#" + method.getName()
                    + " returns void and cannot be used for attribute extraction");
        }
        if (!Modifier.isPublic(method.getDeclaringClass().getModifiers())) {
            method.setAccessible(true);
        }
        return method;
    }

    @Override
    public Field verifyField(Class<?> clazz, Field field) throws ReflectiveAttributeLookupException {
        if (Modifier.isStatic(field.getModifiers())) {
            throw new ReflectiveAttributeLookupException("Field " + clazz.getName() + "#" + field.getName()
                    + " is static and cannot be used for attribute extraction");
        }
        if (!Modifier.isPublic(field.getModifiers()) || !Modifier.isPublic(field.getDeclaringClass().getModifiers())) {
            field.setAccessible(true);
        }
        return field;
    }

    private static Class<?> getNonPublicClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
