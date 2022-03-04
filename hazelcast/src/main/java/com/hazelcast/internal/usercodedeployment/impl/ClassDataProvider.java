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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.nio.IOUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.nio.IOUtil.toByteArray;
import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * Provides {@link ClassData} to remote members.
 *
 * It may consult a local class cache when enabled and then it delegates to a local classloader.
 */
public final class ClassDataProvider {
    private final UserCodeDeploymentConfig.ProviderMode providerMode;
    private final ClassLoader parent;
    private final ConcurrentMap<String, ClassSource> classSourceMap;
    private final ConcurrentMap<String, ClassSource> clientClassSourceMap;
    private final ILogger logger;

    public ClassDataProvider(UserCodeDeploymentConfig.ProviderMode providerMode,
                             ClassLoader parent,
                             ConcurrentMap<String, ClassSource> classSourceMap,
                             ConcurrentMap<String, ClassSource> clientClassSourceMap,
                             ILogger logger) {
        this.providerMode = providerMode;
        this.parent = parent;
        this.classSourceMap = classSourceMap;
        this.clientClassSourceMap = clientClassSourceMap;
        this.logger = logger;
    }

    public ClassData getClassDataOrNull(String className) {
        ClassData classData = loadBytecodesFromClientCache(className);
        if (classData != null) {
            return classData;
        }
        if (providerMode == UserCodeDeploymentConfig.ProviderMode.OFF) {
            return null;
        }
        classData = loadBytecodesFromParent(className);
        if (classData == null && providerMode == UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES) {
            classData = loadBytecodesFromCache(className);
        }
        return classData;
    }

    private ClassData loadBytecodesFromCache(String className) {
        ClassSource classSource = classSourceMap.get(ClassLocator.extractMainClassName(className));
        if (classSource == null) {
            return null;
        }
        return classSource.getClassData(className);
    }

    private ClassData loadBytecodesFromClientCache(String className) {
        ClassSource classSource = clientClassSourceMap.get(ClassLocator.extractMainClassName(className));
        if (classSource == null) {
            return null;
        }
        return classSource.getClassData(className);
    }

    private ClassData loadBytecodesFromParent(String className) {
        byte[] mainClassDefinition = loadBytecodeFromParent(className);
        if (mainClassDefinition == null) {
            return null;
        }

        Map<String, byte[]> innerClassDefinitions = loadInnerClasses(className);
        innerClassDefinitions = loadAnonymousClasses(className, innerClassDefinitions);

        ClassData classData = new ClassData();
        if (innerClassDefinitions != null) {
            classData.setInnerClassDefinitions(innerClassDefinitions);
        }
        classData.setMainClassDefinition(mainClassDefinition);
        return classData;
    }

    private Map<String, byte[]> loadAnonymousClasses(String className, Map<String, byte[]> innerClassDefinitions) {
        int i = 1;

        while (true) {
            String innerClassName = className + "$" + i;
            boolean shouldContinue = attemptToLoadClass(innerClassName);

            if (!shouldContinue) {
                break;
            }

            byte[] innerByteCode = loadBytecodeFromParent(innerClassName);
            if (innerClassDefinitions == null) {
                innerClassDefinitions = new HashMap<String, byte[]>();
            }
            innerClassDefinitions.put(innerClassName, innerByteCode);
            i++;
        }
        return innerClassDefinitions;
    }

    private boolean attemptToLoadClass(String innerClassName) {
        try {
            parent.loadClass(innerClassName);
        } catch (ClassNotFoundException exception) {
            return false;
        }

        return true;
    }

    private Map<String, byte[]> loadInnerClasses(String className) {
        Map<String, byte[]> innerClassDefinitions = null;
        try {
            Class<?> aClass = parent.loadClass(className);
            Class<?>[] declaredClasses = aClass.getDeclaredClasses();
            for (Class<?> declaredClass : declaredClasses) {
                String innerClassName = declaredClass.getName();
                byte[] innerByteCode = loadBytecodeFromParent(innerClassName);
                if (innerClassDefinitions == null) {
                    innerClassDefinitions = new HashMap<String, byte[]>();
                }
                innerClassDefinitions.put(innerClassName, innerByteCode);
            }
        } catch (ClassNotFoundException e) {
            ignore(e);
        }
        return innerClassDefinitions;
    }

    private byte[] loadBytecodeFromParent(String className) {
        String resource = className.replace('.', '/').concat(".class");
        InputStream is = null;
        try {
            is = parent.getResourceAsStream(resource);
            if (is != null) {
                try {
                    return toByteArray(is);
                } catch (IOException e) {
                    logger.severe(e);
                }
            }
        } finally {
            IOUtil.closeResource(is);
        }
        return null;
    }
}
