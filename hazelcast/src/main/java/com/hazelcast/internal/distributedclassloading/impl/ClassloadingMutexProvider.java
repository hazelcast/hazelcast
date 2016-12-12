/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.distributedclassloading.impl;

import com.hazelcast.util.ContextMutexFactory;

import java.io.Closeable;

/**
 * Java 7+ onwards allows parallel classloading. Therefore we can define use a lock with per-class granularity.
 * However in Java 6 we have to use a fat global lock.
 *
 * This abstraction provides a suitable mutex depending on the version of underlying platform.
 *
 * The provided mutexes are closeable as we want to know when the granular mutexes from Java are no longer needed.
 */
public class ClassloadingMutexProvider {
    private static final String JAVA_VERSION_WHERE_PARALLEL_CLASSLOADING_IS_NOT_POSSIBLE = "1.6";
    private static final boolean USE_PARALLEL_LOADING = isParallelClassLoadingPossible();


    private final ContextMutexFactory mutexFactory = new ContextMutexFactory();
    private final GlobalMutex globalMutex = new GlobalMutex();

    public Closeable getMutexForClass(String classname) {
        if (USE_PARALLEL_LOADING) {
            return mutexFactory.mutexFor(classname);
        } else {
            return globalMutex;
        }
    }

    private static boolean isParallelClassLoadingPossible() {
        String implVersion = Runtime.class.getPackage().getImplementationVersion();
        return !implVersion.startsWith(JAVA_VERSION_WHERE_PARALLEL_CLASSLOADING_IS_NOT_POSSIBLE);
    }
}
