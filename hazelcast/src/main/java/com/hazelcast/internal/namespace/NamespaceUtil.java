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

package com.hazelcast.internal.namespace;

import com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.jet.impl.deployment.MapResourceClassLoader;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * Utility to simplify accessing the NamespaceService and Namespace-aware wrapping,
 * as well as provide some useful additional functionality on top of the service
 * implementation, such as providing a default {@link ClassLoader} where specified.
 *
 * @since 5.4
 */
public class NamespaceUtil {

    /** Private constructor to prevent instantiation **/
    private NamespaceUtil() {
    }

    /**
     * Obtains a {@link NodeEngine} reference from {@link NodeEngineThreadLocalContext}
     * and uses it to call {@link NamespaceService#setupNamespace(String)} with the provided
     * parameter.
     *
     * @see NamespaceService#setupNamespace(String)
     */
    public static void setupNamespace(@Nullable String namespace) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        setupNamespace(engine, namespace);
    }

    /**
     * Obtains a {@link NodeEngine} reference from {@link NodeEngineThreadLocalContext}
     * and uses it to call {@link NamespaceService#cleanupNamespace(String)} with the provided
     * parameter.
     *
     * @see NamespaceService#cleanupNamespace(String)
     */
    public static void cleanupNamespace(@Nullable String namespace) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        cleanupNamespace(engine, namespace);
    }

    /**
     * Convenience method for calling the same method name within the {@link NamespaceService},
     * obtained from the provided {@link NodeEngine}.
     *
     * @see NamespaceService#setupNamespace(String)
     */
    public static void setupNamespace(NodeEngine engine, @Nullable String namespace) {
        engine.getNamespaceService().setupNamespace(namespace);
    }

    /**
     * Convenience method for calling the same method name within the {@link NamespaceService},
     * obtained from the provided {@link NodeEngine}.
     *
     * @see NamespaceService#cleanupNamespace(String)
     */
    public static void cleanupNamespace(NodeEngine engine, @Nullable String namespace) {
        engine.getNamespaceService().cleanupNamespace(namespace);
    }

    /**
     * Obtains a {@link NodeEngine} reference from {@link NodeEngineThreadLocalContext}
     * and uses it to call {@link NamespaceService#runWithNamespace(String, Runnable)} with
     * the provided parameters.
     *
     * @see NamespaceService#runWithNamespace(String, Runnable)
     */
    public static void runWithNamespace(@Nullable String namespace, Runnable runnable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        runWithNamespace(engine, namespace, runnable);
    }

    /**
     * Convenience method for calling the same method name within the {@link NamespaceService},
     * obtained from the provided {@link NodeEngine}.
     *
     * @see NamespaceService#runWithNamespace(String, Runnable)
     */
    public static void runWithNamespace(NodeEngine engine, @Nullable String namespace, Runnable runnable) {
        engine.getNamespaceService().runWithNamespace(namespace, runnable);
    }

    /**
     * Obtains a {@link NodeEngine} reference from {@link NodeEngineThreadLocalContext}
     * and uses it to call {@link NamespaceService#callWithNamespace(String, Callable)} with
     * the provided parameters.
     *
     * @see NamespaceService#callWithNamespace(String, Callable)
     */
    public static <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        return callWithNamespace(engine, namespace, callable);
    }

    /**
     * Convenience method for calling the same method name within the {@link NamespaceService},
     * obtained from the provided {@link NodeEngine}.
     *
     * @see NamespaceService#callWithNamespace(String, Callable)
     */
    public static <V> V callWithNamespace(NodeEngine engine, @Nullable String namespace, Callable<V> callable) {
        return engine.getNamespaceService().callWithNamespace(namespace, callable);
    }

    /**
     * Calls the passed {@link Callable} within the {@link ClassLoader} context
     * of the passed {@link Object}'s own {@link ClassLoader} as defined by
     * {@code Object#getClass#getClassLoader()}. The intention is that we can
     * retrieve a User Code Deployment class's {@link MapResourceClassLoader}
     * without the need for any additional references like we would need when
     * fetching using a {@code String namespace}.
     *
     * @implNote This should only be used on UCD objects, as the contract is
     * that all UCD objects are instantiated using the correct Namespace-aware
     * {@link ClassLoader}, allowing this shortcut to work. This also allows us
     * to handle client-executed UCD objects without fuss, as it will simply
     * use their local {@link ClassLoader}.
     *
     * @see #callWithClassLoader(ClassLoader, Callable)
     *
     * @param ucdObject the UCD-instantiated object to retrieve the
     *                  {@link ClassLoader} from for execution
     * @param callable  the {@link Callable} to execute with Namespace awareness
     */
    public static <V> V callWithOwnClassLoader(Object ucdObject, Callable<V> callable) {
        return callWithClassLoader(ucdObject.getClass().getClassLoader(), callable);
    }

    /**
     * Runs the passed {@link Runnable} within the {@link ClassLoader} context
     * of the passed {@link Object}'s own {@link ClassLoader} as defined by
     * {@code Object#getClass#getClassLoader()}. The intention is that we can
     * retrieve a User Code Deployment class's {@link MapResourceClassLoader}
     * without the need for any additional references like we would need when
     * fetching with only a {@code String namespace}.
     *
     * @implNote This should only be used on UCD objects, as the contract is
     * that all UCD objects are instantiated using the correct Namespace-aware
     * {@link ClassLoader}, allowing this shortcut to work. This also allows us
     * to handle client-executed UCD objects without fuss, as it will simply
     * use their local {@link ClassLoader}.
     *
     * @see #runWithClassLoader(ClassLoader, Runnable)
     *
     * @param ucdObject the UCD-instantiated object to retrieve the
     *                  {@link ClassLoader} from for execution
     * @param runnable  the {@link Runnable} to execute with Namespace awareness
     */
    public static void runWithOwnClassLoader(Object ucdObject, Runnable runnable) {
        runWithClassLoader(ucdObject.getClass().getClassLoader(), runnable);
    }

    /**
     * Calls the passed {@link Callable} within the {@link ClassLoader} context
     * of the passed {@link ClassLoader}, leveraging the
     * {@link com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader}.
     *
     * @implNote This is intended to be used with UCD Namespace-aware objects.
     *
     * @param loader    the {@link ClassLoader} to use for execution context
     * @param callable  the {@link Callable} to execute with Namespace awareness
     */
    public static <V> V callWithClassLoader(ClassLoader loader, Callable<V> callable) {
        if (loader == null) {
            try {
                return callable.call();
            } catch (Exception ex) {
                throw sneakyThrow(ex);
            }
        }

        NamespaceThreadLocalContext.onStartNsAware(loader);
        try {
            return callable.call();
        } catch (Exception exception) {
            throw sneakyThrow(exception);
        } finally {
            NamespaceThreadLocalContext.onCompleteNsAware(loader);
        }
    }

    /**
     * Runs the passed {@link Callable} within the {@link ClassLoader} context
     * of the passed {@link ClassLoader}, leveraging the
     * {@link com.hazelcast.internal.namespace.impl.NamespaceAwareClassLoader}.
     *
     * @implNote This is intended to be used with UCD Namespace-aware objects.
     *
     * @param loader    the {@link ClassLoader} to use for execution context
     * @param runnable  the {@link Runnable} to execute with Namespace awareness
     */
    public static void runWithClassLoader(ClassLoader loader, Runnable runnable) {
        if (loader == null) {
            runnable.run();
            return;
        }

        NamespaceThreadLocalContext.onStartNsAware(loader);
        try {
            runnable.run();
        } catch (Exception exception) {
            throw sneakyThrow(exception);
        } finally {
            NamespaceThreadLocalContext.onCompleteNsAware(loader);
        }
    }

    /**
     * Looks for a Namespace associated {@link MapResourceClassLoader} defined by the provided
     * {@code Namespace} name, and returns it if available. If not available, this method
     * will retrieve a default {@link ClassLoader} instance to use as a fallback, as
     * defined by {@link #getDefaultClassloader(NodeEngine)}.
     *
     * @param engine    the {@link NodeEngine} instance to use for accessing the {@link NamespaceService}
     * @param namespace the {@code Namespace} name to use for looking up the Namespace {@link ClassLoader}
     * @return the {@link ClassLoader} for the provided {@code Namespace} if it exists, or else a fallback
     *         {@link ClassLoader} as defined by {@link #getDefaultClassloader(NodeEngine)}.
     */
    public static ClassLoader getClassLoaderForNamespace(NodeEngine engine, @Nullable String namespace) {
        ClassLoader loader = engine.getNamespaceService().getClassLoaderForNamespace(namespace);
        return loader != null ? loader : getDefaultClassloader(engine);
    }

    /**
     * Looks for a Namespace associated {@link MapResourceClassLoader} defined by the provided
     * {@code Namespace} name, and returns it if available. If not available, this method
     * will return the provided {@link ClassLoader}.
     *
     * @param engine        the {@link NodeEngine} instance to use for accessing the {@link NamespaceService}
     * @param namespace     the {@code Namespace} name to use for looking up the Namespace {@link ClassLoader}
     * @param defaultLoader the fallback {@link ClassLoader} to use if a Namespace-associated one is not available.
     * @return the {@link ClassLoader} for the provided {@code Namespace} if it exists, or else the provided
     *         {@link ClassLoader} {@code defaultLoader}.
     */
    public static ClassLoader getClassLoaderForNamespace(NodeEngine engine, @Nullable String namespace,
                                                         ClassLoader defaultLoader) {
        ClassLoader loader = engine.getNamespaceService().getClassLoaderForNamespace(namespace);
        return loader != null ? loader : defaultLoader;
    }

    /**
     * Attempts to retrieve the default {@code Namespace} {@link ClassLoader} if available, otherwise
     * retrieves the config-defined {@link ClassLoader} from {@link NodeEngine#getConfigClassLoader()}.
     * <p>
     * The default Namespace is retrieved by calling {@link NamespaceService#getClassLoaderForNamespace(String)}
     * with a {@code null} Namespace name, which results in the {@link NamespaceService} checking for a
     * default Namespace (defined with name {@link NamespaceService#DEFAULT_NAMESPACE_NAME}).
     *
     * @param engine the {@link NodeEngine} instance to use for accessing the {@link NamespaceService}
     * @return the default {@code Namespace} {@link MapResourceClassLoader} if defined, or the
     *         config-defined {@link ClassLoader} from {@link NodeEngine#getConfigClassLoader()}.
     */
    public static ClassLoader getDefaultClassloader(NodeEngine engine) {
        // Call with `null` namespace, which will fallback to a default Namespace if available
        ClassLoader loader = engine.getNamespaceService().getClassLoaderForNamespace(null);
        return loader != null ? loader : engine.getConfigClassLoader();
    }
}
