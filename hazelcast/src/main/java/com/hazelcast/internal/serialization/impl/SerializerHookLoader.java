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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ServiceLoader;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Loads auto registered serializers using {@link com.hazelcast.nio.serialization.SerializerHook}
 * by reading in the file "META-INF/services/com.hazelcast.SerializerHook" and instantiating
 * the defined SerializerHooks.<br/>
 * This system is meant to be internal code and is subject to change at any time.
 */
final class SerializerHookLoader {

    private static final String FACTORY_ID = "com.hazelcast.SerializerHook";

    private final boolean useDefaultConstructorOnly =
            Boolean.getBoolean("hazelcast.compat.serializers.use.default.constructor.only");

    private final Map<Class, Object> serializers = new HashMap<Class, Object>();
    private final Collection<SerializerConfig> serializerConfigs;
    private final ClassLoader classLoader;

    SerializerHookLoader(SerializationConfig serializationConfig, ClassLoader classLoader) {
        this.serializerConfigs = serializationConfig != null ? serializationConfig.getSerializerConfigs() : null;
        this.classLoader = classLoader;
        load();
    }

    private void load() {
        try {
            final Iterator<SerializerHook> hooks = ServiceLoader.iterator(SerializerHook.class, FACTORY_ID, classLoader);
            while (hooks.hasNext()) {
                final SerializerHook hook = hooks.next();
                final Class serializationType = hook.getSerializationType();
                if (serializationType != null) {
                    serializers.put(serializationType, hook);
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        if (serializerConfigs != null) {
            for (SerializerConfig serializerConfig : serializerConfigs) {
                Serializer serializer = serializerConfig.getImplementation();
                Class serializationType = serializerConfig.getTypeClass();
                if (serializationType == null) {
                    try {
                        serializationType = ClassLoaderUtil.loadClass(classLoader, serializerConfig.getTypeClassName());
                    } catch (ClassNotFoundException e) {
                        throw new HazelcastSerializationException(e);
                    }
                }
                if (serializer == null) {
                    serializer = createSerializerInstance(serializerConfig, serializationType);
                }
                register(serializationType, serializer);
            }
        }
    }

    private Serializer createSerializerInstance(SerializerConfig serializerConfig, Class serializationType) {
        try {
            String className = serializerConfig.getClassName();
            if (useDefaultConstructorOnly) {
                return ClassLoaderUtil.newInstance(classLoader, className);
            } else {
                return createSerializerInstanceWithFallback(serializationType, className);
            }
        } catch (Exception e) {
            throw new HazelcastSerializationException(e);
        }
    }

    private Serializer createSerializerInstanceWithFallback(Class serializationType, String className) throws Exception {
        Class<?> clazz = ClassLoaderUtil.loadClass(classLoader, className);
        try {
            Constructor<?> constructor = clazz.getDeclaredConstructor(Class.class);
            constructor.setAccessible(true);
            return (Serializer) constructor.newInstance(serializationType);
        } catch (NoSuchMethodException e) {
            //fallback to no-arg constructor
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (Serializer) constructor.newInstance();
        }
    }

    Map<Class, Object> getSerializers() {
        return serializers;
    }

    private void register(Class serializationType, Serializer serializer) {
        final Object current = serializers.get(serializationType);
        if (current != null) {
            if (current.equals(serializer)) {
                Logger.getLogger(getClass()).warning("Serializer[" + serializationType.toString()
                        + "] is already registered! Skipping " + serializer);
            } else if (current instanceof SerializerHook && ((SerializerHook) current).isOverwritable()) {
                serializers.put(serializationType, serializer);
            } else {
                throw new IllegalArgumentException("Serializer[" + serializationType.toString()
                        + "] is already registered! " + current + " -> " + serializer);
            }
        } else {
            serializers.put(serializationType, serializer);
        }
    }

}
