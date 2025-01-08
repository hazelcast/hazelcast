/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.internal.namespace.UserCodeNamespaceService;

import javax.annotation.Nullable;

/**
 * An interface to mark implementing configs as "Namespace aware", meaning they should
 * support the definition of a {@code User Code Namespace} string which represents the
 * User Code Namespace to associate with all operations pertaining to this config.
 *
 * @since 5.4
 * @param <T> Implementation type for use in return type of {@link #setUserCodeNamespace(String)}
 */
public interface UserCodeNamespaceAwareConfig<T> {
    /**
     * Defines the default User Code Namespace Name used for all {@link UserCodeNamespaceAwareConfig} implementations.
     * <p>
     * The default is currently {@code null} which results in the following behaviour:<ul>
     *   <li>When a "default" Namespace is defined in the {@link UserCodeNamespaceService},
     *     i.e. a Namespace exists with the ID {@link UserCodeNamespaceService#DEFAULT_NAMESPACE_NAME},
     *     that Namespace is used for relevant operations after being transformed internally.</li>
     *   <li>When there is no "default" Namespace defined, this value remains {@code null} and there is
     *     no Namespace awareness used during relevant operations.</li>
     * </ul>
     */
    String DEFAULT_NAMESPACE = null;

    /**
     * Retrieve the User Code Deployment Namespace to be used for {@link ClassLoader} awareness
     * during operations related to the structure associated with this configuration.
     *
     * @return Namespace Name for use with the {@link UserCodeNamespaceService},
     *         or {@code null} if there is no User Code Namespace to associate with.
     * @since 5.4
     */
    @Nullable
    default String getUserCodeNamespace() {
        return DEFAULT_NAMESPACE;
    }

    /**
     * Associates the provided User Code Namespace Name with this structure for {@link ClassLoader} awareness.
     * <p>
     * The behaviour of setting this to {@code null} is outlined in the documentation for
     * {@link UserCodeNamespaceAwareConfig#DEFAULT_NAMESPACE}.
     *
     * @param userCodeNamespace The ID of the User Code Namespace to associate with this structure.
     * @return the updated {@link T} config instance
     * @since 5.4
     */
    T setUserCodeNamespace(@Nullable String userCodeNamespace);
}
