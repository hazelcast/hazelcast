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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentService;
import com.hazelcast.internal.usercodedeployment.impl.operation.ClassDataFinderOperation;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.filter.Filter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.io.Closeable;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.security.AccessController.doPrivileged;

/**
 * Provides classes to a local member.
 *
 * It's called by {@link UserCodeDeploymentClassLoader} when a class
 * is not found on local classpath.
 *
 * The current implementation can consult the cache and when the class is not found then it consults
 * remote members.
 */
public final class ClassLocator {
    private static final Pattern CLASS_PATTERN = Pattern.compile("^(.*)\\$.*");
    private final ConcurrentMap<String, ClassSource> classSourceMap;
    private final ConcurrentMap<String, ClassSource> clientClassSourceMap;
    private final ClassLoader parent;
    private final Filter<String> classNameFilter;
    private final Filter<Member> memberFilter;
    private final UserCodeDeploymentConfig.ClassCacheMode classCacheMode;
    private final NodeEngine nodeEngine;
    private final ContextMutexFactory mutexFactory = new ContextMutexFactory();
    private final ILogger logger;

    public ClassLocator(ConcurrentMap<String, ClassSource> classSourceMap,
                        ConcurrentMap<String, ClassSource> clientClassSourceMap,
                        ClassLoader parent, Filter<String> classNameFilter, Filter<Member> memberFilter,
                        UserCodeDeploymentConfig.ClassCacheMode classCacheMode, NodeEngine nodeEngine) {
        this.classSourceMap = classSourceMap;
        this.clientClassSourceMap = clientClassSourceMap;
        this.parent = parent;
        this.classNameFilter = classNameFilter;
        this.memberFilter = memberFilter;
        this.classCacheMode = classCacheMode;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ClassLocator.class);
    }

    public static void onStartDeserialization() {
        ThreadLocalClassCache.onStartDeserialization();
    }

    public static void onFinishDeserialization() {
        ThreadLocalClassCache.onFinishDeserialization();
    }

    public Class<?> handleClassNotFoundException(String name) throws ClassNotFoundException {
        if (!classNameFilter.accept(name)) {
            throw new ClassNotFoundException("Class " + name + " is not allowed to be loaded from other members");
        }
        Class<?> clazz = tryToGetClassFromLocalCache(name);
        if (clazz != null) {
            return clazz;
        }
        return tryToGetClassFromRemote(name);
    }

    public void defineClassesFromClient(List<Map.Entry<String, byte[]>> bundledClassDefinitions) {
        Map<String, byte[]> bundledClassDefMap = new HashMap<>();
        for (Map.Entry<String, byte[]> bundledClassDefinition : bundledClassDefinitions) {
            bundledClassDefMap.put(bundledClassDefinition.getKey(), bundledClassDefinition.getValue());
        }
        for (Map.Entry<String, byte[]> bundledClassDefinition : bundledClassDefinitions) {
            defineClassFromClient(bundledClassDefinition.getKey(), bundledClassDefinition.getValue(), bundledClassDefMap);
        }
    }

    public Class<?> defineClassFromClient(final String name, final byte[] classDef, Map<String, byte[]> bundledClassDefinitions) {
        // we need to acquire a classloading lock before defining a class
        // Java 7+ can use locks with per-class granularity while Java 6 has to use a single lock
        // mutexFactory abstract these differences away
        String mainClassName = extractMainClassName(name);
        Closeable classMutex = mutexFactory.mutexFor(mainClassName);
        try {
            synchronized (classMutex) {
                ClassSource classSource = clientClassSourceMap.get(mainClassName);
                if (classSource != null) {
                    if (classSource.getClazz(name) != null) {
                        if (!Arrays.equals(classDef, classSource.getClassDefinition(name))) {
                            throw new IllegalStateException("Class " + name
                                    + " is already in local cache and has conflicting byte code representation");
                        } else if (logger.isFineEnabled()) {
                            logger.finest("Class " + name + " is already in local cache with equal byte code");
                        }
                        return classSource.getClazz(name);
                    }
                } else {
                    classSource = doPrivileged((PrivilegedAction<ClassSource>) ()
                            -> new ClassSource(parent, ClassLocator.this, bundledClassDefinitions));
                    clientClassSourceMap.put(mainClassName, classSource);
                }
                return classSource.define(name, classDef);
            }
        } finally {
            IOUtil.closeResource(classMutex);
        }
    }

    private Class<?> tryToGetClassFromRemote(String name) throws ClassNotFoundException {
        // we need to acquire a classloading lock before defining a class
        // Java 7+ can use locks with per-class granularity while Java 6 has to use a single lock
        // mutexFactory abstract these differences away
        String mainClassName = extractMainClassName(name);
        Closeable classMutex = mutexFactory.mutexFor(mainClassName);
        try {
            synchronized (classMutex) {
                ClassSource classSource = classSourceMap.get(mainClassName);
                if (classSource != null) {
                    Class clazz = classSource.getClazz(name);
                    if (clazz != null) {
                        if (logger.isFineEnabled()) {
                            logger.finest("Class " + name + " is already in local cache");
                        }
                        return clazz;
                    }
                } else if (ThreadLocalClassCache.getFromCache(mainClassName) != null) {
                    classSource = ThreadLocalClassCache.getFromCache(mainClassName);
                } else {
                    classSource = doPrivileged(
                            (PrivilegedAction<ClassSource>) () -> new ClassSource(parent, this, Collections.emptyMap()));
                }
                ClassData classData = fetchBytecodeFromRemote(mainClassName);
                if (classData == null) {
                    throw new ClassNotFoundException("Failed to load class " + name + " from other members");
                }

                Map<String, byte[]> innerClassDefinitions = classData.getInnerClassDefinitions();
                classSource.define(mainClassName, classData.getMainClassDefinition());
                for (Map.Entry<String, byte[]> entry : innerClassDefinitions.entrySet()) {
                    classSource.define(entry.getKey(), entry.getValue());
                }
                cacheClass(classSource, mainClassName);
                return classSource.getClazz(name);
            }
        } finally {
            IOUtil.closeResource(classMutex);
        }
    }

    private Class<?> tryToGetClassFromLocalCache(String name) {
        String mainClassDefinition = extractMainClassName(name);
        ClassSource classSource = classSourceMap.get(mainClassDefinition);
        if (classSource != null) {
            Class clazz = classSource.getClazz(name);
            if (clazz != null) {
                if (logger.isFineEnabled()) {
                    logger.finest("Class " + name + " is already in local cache");
                }
                return clazz;
            }
        }

        classSource = clientClassSourceMap.get(mainClassDefinition);
        if (classSource != null) {
            Class clazz = classSource.getClazz(name);
            if (clazz != null) {
                if (logger.isFineEnabled()) {
                    logger.finest("Class " + name + " is already in local cache");
                }
                return clazz;
            }
        }

        classSource = ThreadLocalClassCache.getFromCache(mainClassDefinition);
        if (classSource != null) {
            return classSource.getClazz(name);
        }
        return null;
    }

    static String extractMainClassName(String className) {
        Matcher matcher = CLASS_PATTERN.matcher(className);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return className;
    }

    // called while holding class lock
    private void cacheClass(ClassSource classSource, String outerClassName) {
        if (classCacheMode != UserCodeDeploymentConfig.ClassCacheMode.OFF) {
            classSourceMap.put(outerClassName, classSource);
        } else {
            ThreadLocalClassCache.store(outerClassName, classSource);
        }
    }

    // called while holding class lock
    private ClassData fetchBytecodeFromRemote(String className) {
        ClusterService cluster = nodeEngine.getClusterService();
        ClassData classData;
        boolean interrupted = false;
        for (Member member : cluster.getMembers()) {
            if (!isCandidateMember(member)) {
                continue;
            }
            try {
                classData = tryToFetchClassDataFromMember(className, member);
                if (classData != null) {
                    if (logger.isFineEnabled()) {
                        logger.finest("Loaded class " + className + " from " + member);
                    }
                    return classData;
                }
            } catch (InterruptedException e) {
                // question: should we give up on loading at this point and simply throw ClassNotFoundException?
                interrupted = true;
            } catch (Exception e) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Unable to get class data for class " + className
                            + " from member " + member, e);
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    private ClassData tryToFetchClassDataFromMember(String className, Member member) throws ExecutionException,
            InterruptedException {
        OperationService operationService = nodeEngine.getOperationService();

        ClassDataFinderOperation op = new ClassDataFinderOperation(className);
        Future<ClassData> classDataFuture = operationService.invokeOnTarget(UserCodeDeploymentService.SERVICE_NAME,
                op, member.getAddress());
        return classDataFuture.get();
    }

    private boolean isCandidateMember(Member member) {
        return !member.localMember()
                && memberFilter.accept(member);
    }

    public Class<?> findLoadedClass(String name) {
        ClassSource classSource = classSourceMap.get(extractMainClassName(name));
        if (classSource == null) {
            return null;
        }
        return classSource.getClazz(name);
    }
}
