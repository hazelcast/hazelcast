/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.UserCodeNamespaceConfig;
import com.hazelcast.config.UserCodeNamespacesConfig;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.monitor.impl.LocalUserCodeNamespaceStats;
import com.hazelcast.internal.services.StatisticsAwareService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * Primary service used to register and maintain {@code User Code Namespace} definitions,
 * as well as the centralised location for handling {@code Namespace} awareness.
 *
 * @see NamespaceUtil
 * @since 5.4
 */
public interface UserCodeNamespaceService extends StatisticsAwareService<LocalUserCodeNamespaceStats>, DynamicMetricsProvider {
    /** Name of the User Code Namespace Service */
    String SERVICE_NAME = "hz:impl:namespaceService";
    /**
     * The default Namespace name to be used when a Namespace-aware execution takes place
     * without an explicitly defined Namespace. This Namespace can be defined in the
     * config for use, and if not defined then no Namespace awareness will be provided.
     */
    String DEFAULT_NAMESPACE_NAME = "default";

    /**
     * Defines a new {@code Namespace} with the provided Namespace name and available resources.
     * If a {@code Namespace} already exists with this name, then the existing Namespace will
     * be overwritten with this new implementation (add-or-replace functionality) and all new
     * UDF invocations requesting Namespace awareness will be executed with the newly provided
     * resources available instead.
     *
     * @param nsName    the name of the {@code Namespace}
     * @param resources the resources to associate with the {@code Namespace}
     */
    void addNamespace(@Nonnull String nsName,
                      @Nonnull Collection<ResourceDefinition> resources);

    /**
     * Checks to see if there is a {@code Namespace} defined with the provided name.
     *
     * @param namespace the name of the {@code Namespace} to check for
     * @return {@code true} if a {@code Namespace} exists with that name, otherwise {@code false}.
     */
    boolean hasNamespace(String namespace);

    /**
     * Fast checking method to see if the underlying implementation is a no-op implementation
     * (<b>not enabled</b>), or an actual implementation (<b>enabled</b>).
     *
     * @return {@code true} if the underlying implementation is functional, otherwise {@code false}
     */
    boolean isEnabled();

    /**
     * In order to fail-fast, we skip Namespace-awareness handling when an object's namespace is
     * `null` - however, if we have a default Namespace defined, we should use that in these cases.
     * To facilitate failing fast with minimal overhead, we track this separately.
     *
     * @return {@code true} if a default Namespace exists, otherwise {@code false}
     */
    boolean isDefaultNamespaceDefined();

    /**
     * Prepares {@code Namespace} awareness context on the current thread for the provided
     * {@code Namespace} name if available, or does nothing if there is none defined.
     * <p>
     * Specifically, this method will look for a defined {@code Namespace} from the passed
     * name, and if it exists, it will set it as the {@link ThreadLocal} reference available
     * in this thread's {@link com.hazelcast.internal.namespace.impl.NamespaceThreadLocalContext}.
     * <p>
     * @implNote This method will transform the provided {@code Namespace} name to be the
     * {@link #DEFAULT_NAMESPACE_NAME} if it is passed as {@code null} and the default {@code Namespace}
     * exists, as determined by {@link #isDefaultNamespaceDefined()}.
     *
     * @param namespace the {@code Namespace} name to set up awareness with.
     */
    void setupNamespace(@Nullable String namespace);

    /**
     * Cleans up {@code Namespace} awareness context on the current thread for the provided
     * {@code Namespace} name if applicable, or does nothing if there is no context to clean up.
     * <p>
     * @implNote This method will transform the provided {@code Namespace} name to be the
     * {@link #DEFAULT_NAMESPACE_NAME} if it is passed as {@code null} and the default {@code Namespace}
     * exists, as determined by {@link #isDefaultNamespaceDefined()}.
     *
     * @param namespace the {@code Namespace} name to clean up awareness from.
     */
    void cleanupNamespace(@Nullable String namespace);

    /**
     * Runs the provided {@link Runnable} with the {@code Namespace} context of the {@code Namespace}
     * context defined by the provided Namespace name if available. If there is no Namespace available
     * then the {@link Runnable} is executed as if it was called directly by the caller.
     * <p>
     * Calling this method is the equivalent of running the following code:
     * <pre>
     * {@code
     * NamespaceService service = node.getNamespaceService();
     * try {
     *     service.setupNamespace(namespace);
     *     runnable.run();
     * } finally {
     *     service.cleanupNamespace(namespace);
     * }
     * }
     * </pre>
     * <p>
     * @implNote This method will transform the provided {@code Namespace} name to be the
     * {@link #DEFAULT_NAMESPACE_NAME} if it is passed as {@code null} and the default {@code Namespace}
     * exists, as determined by {@link #isDefaultNamespaceDefined()}.
     *
     * @param namespace the {@code Namespace} name to use for finding the appropriate {@code Namespace}.
     * @param runnable  the {@link Runnable} to execute with Namespace awareness
     */
    void runWithNamespace(@Nullable String namespace, Runnable runnable);

    /**
     * Runs the provided {@link Callable} with the {@code Namespace} context of the {@code Namespace}
     * context defined by the provided Namespace name if available. If there is no Namespace available
     * then the {@link Callable} is executed as if it was called directly by the caller.
     * <p>
     * Calling this method is the equivalent of running the following code:
     * <pre>
     * {@code
     * NamespaceService service = node.getNamespaceService();
     * try {
     *     service.setupNamespace(namespace);
     *     return callable.call();
     * } finally {
     *     service.cleanupNamespace(namespace);
     * }
     * }
     * </pre>
     * <p>
     * @implNote This method will transform the provided {@code Namespace} name to be the
     * {@link #DEFAULT_NAMESPACE_NAME} if it is passed as {@code null} and the default {@code Namespace}
     * exists, as determined by {@link #isDefaultNamespaceDefined()}.
     *
     * @param namespace the {@code Namespace} name to use for finding the appropriate {@code Namespace}.
     * @param callable  the {@link Callable} to execute with Namespace awareness.
     * @param <V>       the {@link Callable}'s return type.
     * @return the completed result of the provided {@link Callable}
     */
    <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable);

    /**
     * Looks up the {@code Namespace} associated with the provided Namespace name and returns
     * the {@link com.hazelcast.jet.impl.deployment.MapResourceClassLoader} defined for said
     * {@code Namespace} if it exists. If it does not exist, then {@code null} is returned.
     * <p>
     * @implNote This method will transform the provided {@code Namespace} name to be the
     * {@link #DEFAULT_NAMESPACE_NAME} if it is passed as {@code null} and the default {@code Namespace}
     * exists, as determined by {@link #isDefaultNamespaceDefined()}.
     *
     * @param namespace the {@code Namespace} name to use looking up associated {@code Namespace}.
     * @return the  associated{@link ClassLoader} if the {@code Namespace} exists, otherwise {@code null}.
     */
    ClassLoader getClassLoaderForNamespace(String namespace);

    /**
     * Convenience method for adding {@code Namespace}s directly using a
     * {@link UserCodeNamespaceConfig} instance.
     * <p>
     * This method will add the {@code Namespace} to the provided {@link UserCodeNamespacesConfig}
     * but will not broadcast the change to cluster members.
     *
     * @param namespacesConfig the {@link UserCodeNamespacesConfig} instance to add the namespace to.
     * @param namespaceConfig the {@link UserCodeNamespaceConfig} instance to fetch the {@code Namespace}
     *               name from for addition via {@link #addNamespace(String, Collection)}
     */
    default void addNamespaceConfig(UserCodeNamespacesConfig namespacesConfig, UserCodeNamespaceConfig namespaceConfig) {
        ConfigAccessor.addNamespaceConfigLocally(namespacesConfig, namespaceConfig);
        addNamespace(namespaceConfig.getName(), ConfigAccessor.getResourceDefinitions(namespaceConfig));
    }
}
