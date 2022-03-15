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

package com.hazelcast.test.backup;

import java.util.concurrent.Callable;

/**
 * Base class for {@link Callable} classes which need a special TCCL to be set.
 *
 * @param <V> the result type of method {@code call}
 */
abstract class AbstractClassLoaderAwareCallable<V> implements Callable<V> {

    private final ClassLoader classLoader = AbstractClassLoaderAwareCallable.class.getClassLoader();

    @Override
    public final V call() throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader contextClassLoader = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(classLoader);
            return callInternal();
        } finally {
            thread.setContextClassLoader(contextClassLoader);
        }
    }

    abstract V callInternal() throws Exception;
}
