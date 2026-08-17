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

package com.hazelcast.spi.impl;

/**
 * A thread-local context that provides access to a {@link NodeEngine}
 * instance when no local variable is available in the current execution context.
 * <p>
 * The use of this thread-local context allows us to reduce code complexity that would otherwise
 * be introduced by passing {@link NodeEngine} references through long function chains.
 * <p>
 * Not all threads have a {@link NodeEngine} bound to this context. It is
 * currently safe to use in operation threads, where the context is explicitly set.
 */
public final class NodeEngineThreadLocalContext {

    private static final ThreadLocal<NodeEngine> NE_THREAD_LOCAL = new ThreadLocal<>();

    private NodeEngineThreadLocalContext() {
    }

    /**
     * Sets the provided {@link NodeEngine} reference as this thread's {@link ThreadLocal} instance.
     *
     * @param nodeEngine the {@link NodeEngine} reference to use.
     */
    public static void declareNodeEngineReference(NodeEngine nodeEngine) {
        if (nodeEngine != null) {
            NE_THREAD_LOCAL.set(nodeEngine);
        }
    }

    /**
     * Removes the currently set {@link NodeEngine} reference for this thread.
     */
    public static void destroyNodeEngineReference() {
        NE_THREAD_LOCAL.remove();
    }

    /**
     * Retrieves the currently set {@link NodeEngine} reference for this thread,
     * or throws an {@link IllegalStateException} if one could not be found.
     *
     * @return This thread's {@link NodeEngine} reference.
     */
    public static NodeEngine getNodeEngineThreadLocalContext() {
        NodeEngine tlContext = NE_THREAD_LOCAL.get();
        if (tlContext == null) {
            throw new IllegalStateException("NodeEngine context is not available for the current thread! Current thread: "
                    + Thread.currentThread().getName() + " (" + Thread.currentThread().getId() + ")");
        } else {
            return tlContext;
        }
    }

    /**
     * Retrieves the currently set {@link NodeEngine} reference for this thread,
     * or {@code null} if one could not be found.
     *
     * @return This thread's {@link NodeEngine} reference if available, or {@code null}.
     */
    public static NodeEngine getNodeEngineThreadLocalContextOrNull() {
        return NE_THREAD_LOCAL.get();
    }
}
