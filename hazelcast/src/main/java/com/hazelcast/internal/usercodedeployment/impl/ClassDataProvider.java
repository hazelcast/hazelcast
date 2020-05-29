/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
        if (logger.isFinestEnabled()) {
            logger.finest("Searching ClassData for " + className);
        }
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

        Map<String, byte[]> innerClassDefinitions = new HashMap<String, byte[]>();
        loadInnerClasses(className, innerClassDefinitions);

        ClassData classData = new ClassData();
        if (! innerClassDefinitions.isEmpty()) {
            classData.setInnerClassDefinitions(innerClassDefinitions);
        }
        classData.setMainClassDefinition(mainClassDefinition);
        return classData;
    }

    private Map<String, byte[]> loadAnonymousClasses(String className, Map<String, byte[]> innerClassDefinitions) {
        int i = 1;
        while (true) {
            try {
                String innerClassName = className + "$" + i;
                Class<?> anonClass = parent.loadClass(innerClassName);
                addInnerClass(innerClassDefinitions, innerClassName);
                loadProtectedParents(anonClass, innerClassDefinitions);
                i++;
            } catch (ClassNotFoundException e) {
                break;
            }
        }
        return innerClassDefinitions;
    }

    private Map<String, byte[]> loadProtectedParents(Class<?> clazz, Map<String, byte[]> innerClassDefinitions) {
        Class<?> parentClass = clazz.getSuperclass();
        while (parentClass != null && !Modifier.isPublic(parentClass.getModifiers())) {
            String name = parentClass.getName();
            addInnerClass(innerClassDefinitions, name);
            loadInnerClasses(name, innerClassDefinitions);
            parentClass = parentClass.getSuperclass();
        }
        for (Class<?> ifaceClass : getInterfaces(clazz, new HashSet<>())) {
            if (!Modifier.isPublic(ifaceClass.getModifiers())) {
                String name = ifaceClass.getName();
                addInnerClass(innerClassDefinitions, name);
                loadInnerClasses(name, innerClassDefinitions);
            }
        }
        return innerClassDefinitions;
    }

    protected void addInnerClass(Map<String, byte[]> innerClassDefinitions, String name) {
        if (!innerClassDefinitions.containsKey(name)) {
            byte[] byteCode = loadBytecodeFromParent(name);
            innerClassDefinitions.put(name, byteCode);
        }
    }

    private Set<Class<?>> getInterfaces(Class<?> clazz, Set<Class<?>> ifSet) {
        while (clazz != null) {
            for (Class<?> declaredIf : clazz.getInterfaces()) {
                if (ifSet.add(declaredIf)) {
                    getInterfaces(declaredIf, ifSet);
                }
            }
            clazz = clazz.getSuperclass();
        }
        return ifSet;
    }

    private Map<String, byte[]> loadInnerClasses(String className, Map<String, byte[]> innerClassDefinitions) {
        try {
            Class<?> aClass = parent.loadClass(className);
            innerClassDefinitions = loadProtectedParents(aClass, innerClassDefinitions);
            loadAnonymousClasses(className, innerClassDefinitions);
            Class<?>[] declaredClasses = aClass.getDeclaredClasses();
            for (Class<?> declaredClass : declaredClasses) {
                String name = declaredClass.getName();
                addInnerClass(innerClassDefinitions, name);
                loadProtectedParents(declaredClass, innerClassDefinitions);
                loadInnerClasses(name, innerClassDefinitions);
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
