/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A thread-local context that maintains a {@link ClassLoader} instance for use in providing
 * Namespace awareness to areas of execution that require it.
 * <p><
 * Must be setup around user-code serde in client messages, and execution on members.
 * Additionally, should be propagated via member-to-member operations.
 */
public final class NamespaceThreadLocalContext {
    private static final ThreadLocal<NamespaceThreadLocalContext> NS_THREAD_LOCAL = new ThreadLocal<>();

    private final ClassLoader classLoader;
    private int counter = 1;
    private NamespaceThreadLocalContext previous;

    private NamespaceThreadLocalContext(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    private NamespaceThreadLocalContext(ClassLoader classLoader, NamespaceThreadLocalContext previous) {
        this.classLoader = classLoader;
        this.previous = previous;
    }

    private void incCounter() {
        counter++;
    }

    private int decCounter() {
        return --counter;
    }

    @Override
    public String toString() {
        return "NamespaceThreadLocalContext{"
                + "classLoader=" + classLoader
                + ", counter=" + counter
                + '}';
    }

    /**
     * Sets the provided {@link ClassLoader} as this thread's {@link ThreadLocal}
     * loader instance, to be used for Namespace aware class loading.
     * <p>
     * @implNote It is important that <b>context is cleaned up after</b> by invoking
     * either {@link #onCompleteNsAware(ClassLoader)} or {@link #onCompleteNsAware(String)}.
     *
     * @param classLoader the {@link ClassLoader} to use for Namespace awareness.
     */
    public static void onStartNsAware(ClassLoader classLoader) {
        assert classLoader != null;
        NamespaceThreadLocalContext tlContext = NS_THREAD_LOCAL.get();
        if (tlContext == null) {
            tlContext = new NamespaceThreadLocalContext(classLoader);
            NS_THREAD_LOCAL.set(tlContext);
        } else {
            if (!tlContext.classLoader.equals(classLoader)) {
                // Allow for ClassLoader overwrite, but allow for return by retaining the current context, linked list style
                tlContext = new NamespaceThreadLocalContext(classLoader, tlContext);
                NS_THREAD_LOCAL.set(tlContext);
                return;
            }
            tlContext.incCounter();
        }
    }

    /**
     * Removes the currently set {@link ClassLoader} from this thread's {@link ThreadLocal}
     * variable, if it matches the provided {@link ClassLoader} instance.
     *
     * @param classLoader the {@link ClassLoader} to expect when removing.
     */
    public static void onCompleteNsAware(ClassLoader classLoader) {
        onCompleteNsAware(tlContext -> Objects.equals(tlContext.classLoader, classLoader),
                tlContext -> "Attempted to complete NSTLContext for classLoader " + classLoader
                        + " but there is an existing context: " + tlContext);
    }

    /**
     * Removes the currently set {@link ClassLoader} from this thread's {@link ThreadLocal}
     * variable, if it's {@link MapResourceClassLoader#getUserCodeNamespace()} matches the provided
     * {@code Namespace} ID.
     *
     * @param namespace the {@code Namespace} ID to expect when removing.
     */
    public static void onCompleteNsAware(String namespace) {
        onCompleteNsAware(tlContext -> tlContext.classLoader instanceof MapResourceClassLoader
                        && Objects.equals(((MapResourceClassLoader) tlContext.classLoader).getUserCodeNamespace(), namespace),
                tlContext -> "Attempted to complete NSTLContext for namespace " + namespace
                        + " but there is an existing context: " + tlContext);
    }

    private static void onCompleteNsAware(Predicate<NamespaceThreadLocalContext> equalityFunc,
                                          Function<NamespaceThreadLocalContext, String> errorMessageFunc) {
        NamespaceThreadLocalContext tlContext = NS_THREAD_LOCAL.get();
        if (tlContext != null) {
            if (!equalityFunc.test(tlContext)) {
                throw new IllegalStateException(errorMessageFunc.apply(tlContext));
            }
            int count = tlContext.decCounter();
            if (count == 0) {
                // Check for linked previous to revert to
                if (tlContext.previous != null) {
                    NS_THREAD_LOCAL.set(tlContext.previous);
                    tlContext.previous = null;
                } else {
                    NS_THREAD_LOCAL.remove();
                }
            }
        }
    }

    /**
     * Retrieves the {@link ClassLoader} currently stored within this
     * thread's {@link ThreadLocal} variable.
     *
     * @return the {@link ClassLoader} instance if available, or {@code null}.
     */
    public static ClassLoader getClassLoader() {
        NamespaceThreadLocalContext tlContext = NS_THREAD_LOCAL.get();
        if (tlContext == null) {
            // No context, no namespace wrapping (not even default)
            return null;
        } else {
            return tlContext.classLoader;
        }
    }
}
