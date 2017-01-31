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

package com.hazelcast.internal.usercodedeployment.impl;

import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentService;
import com.hazelcast.internal.usercodedeployment.impl.operation.ClassDataFinderOperation;
import com.hazelcast.internal.util.filter.Filter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Provides classes to a local member.
 *
 * It's called by {@link UserCodeDeploymentClassLoader} when a class
 * is not found on a local classpath.
 *
 * The current implementation can consult a cache and when the class is not found then it consults
 * remote members.
 */
public final class ClassLocator {
    private final ConcurrentMap<String, ClassSource> classSourceMap;
    private final ClassLoader parent;
    private final Filter<String> classNameFilter;
    private final Filter<Member> memberFilter;
    private final UserCodeDeploymentConfig.ClassCacheMode classCacheMode;
    private final NodeEngine nodeEngine;
    private final ClassloadingMutexProvider mutexFactory = new ClassloadingMutexProvider();
    private final ILogger logger;

    public ClassLocator(ConcurrentMap<String, ClassSource> classSourceMap,
                        ClassLoader parent, Filter<String> classNameFilter, Filter<Member> memberFilter,
                        UserCodeDeploymentConfig.ClassCacheMode classCacheMode, NodeEngine nodeEngine) {
        this.classSourceMap = classSourceMap;
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

    public Class<?> handleClassNotFoundException(String name)
            throws ClassNotFoundException {
        if (!classNameFilter.accept(name)) {
            throw new ClassNotFoundException("Class " + name + " is not allowed to be loaded from other members.");
        }
        Class<?> clazz = tryToGetClassFromLocalCache(name);
        if (clazz != null) {
            return clazz;
        }
        return tryToGetClassFromRemote(name);
    }

    private Class<?> tryToGetClassFromRemote(String name) throws ClassNotFoundException {
        // we need to acquire a classloading lock before defining a class
        // Java 7+ can use locks with per-class granularity while Java 6 has to use a single lock
        // mutexFactory abstract these differences away
        Closeable classMutex = mutexFactory.getMutexForClass(name);
        try {
            synchronized (classMutex) {
                ClassSource classSource = classSourceMap.get(name);
                if (classSource != null) {
                    if (logger.isFineEnabled()) {
                        logger.finest("Class " + name + " is already in a local cache. ");
                    }
                    return classSource.getClazz();
                }
                byte[] classDef = fetchBytecodeFromRemote(name);
                if (classDef == null) {
                    throw new ClassNotFoundException("Failed to load class " + name + " from other members.");
                }
                return defineAndCacheClass(name, classDef);
            }
        } finally {
            IOUtil.closeResource(classMutex);
        }
    }

    private Class<?> tryToGetClassFromLocalCache(String name) {
        ClassSource classSource = classSourceMap.get(name);
        if (classSource != null) {
            if (logger.isFineEnabled()) {
                logger.finest("Class " + name + " is already in a local cache. ");
            }
            return classSource.getClazz();
        }

        classSource = ThreadLocalClassCache.getFromCache(name);
        if (classSource != null) {
            return classSource.getClazz();
        }
        return null;
    }

    // called while holding class lock
    private Class<?> defineAndCacheClass(String name, byte[] classDef) {
        ClassSource classSource = new ClassSource(name, classDef, parent, this);
        classSource.define();
        if (classCacheMode != UserCodeDeploymentConfig.ClassCacheMode.OFF) {
            classSourceMap.put(name, classSource);
        } else {
            ThreadLocalClassCache.store(name, classSource);
        }
        return classSource.getClazz();
    }

    // called while holding class lock
    private byte[] fetchBytecodeFromRemote(String className) {
        ClusterService cluster = nodeEngine.getClusterService();
        ClassData classData;
        boolean interrupted = false;
        for (Member member : cluster.getMembers()) {
            if (isCandidateMember(member)) {
                continue;
            }
            try {
                classData = tryToFetchClassDataFromMember(className, member);
                if (classData != null) {
                    if (logger.isFineEnabled()) {
                        logger.finest("Loaded class " + className + " from " + member);
                    }
                    byte[] classDef = classData.getClassDefinition();
                    if (classDef != null) {
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return classDef;
                    }
                }
            } catch (InterruptedException e) {
                // question: should we give-up on loading and this point and simply throw ClassNotFoundException?
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
        if (member.localMember()) {
            return true;
        }
        if (!memberFilter.accept(member)) {
            return true;
        }
        return false;
    }

    public Class<?> findLoadedClass(String name) {
        ClassSource classSource = classSourceMap.get(name);
        if (classSource == null) {
            return null;
        }
        return classSource.getClazz();
    }
}
