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

package com.hazelcast.config;

import javax.annotation.Nullable;

public interface NamespaceAwareConfig {
    /**
     * Defines the default Namespace Name used for all {@link NamespaceAwareConfig} implementations.
     * <p>
     * The default is currently {@code null} which results in the following behaviour, as defined
     * by the {@link com.hazelcast.internal.namespace.impl.NamespaceServiceImpl} implementation:
     *   * When a "default" Namespace is defined in the {@code NamespaceService}, i.e. a Namespace
     *     exists with the ID {@link com.hazelcast.internal.namespace.NamespaceService#DEFAULT_NAMESPACE_NAME},
     *     that Namespace is used for relevant operations after being transformed by the function
     *     {@link com.hazelcast.internal.namespace.impl.NamespaceServiceImpl#transformNamespace(String)}.
     *   * When there is no "default" Namespace defined, this value remains {@code null} and there is
     *     no Namespace awareness used during relevant operations.
     */
    String DEFAULT_NAMESPACE = null;

    /**
     * Retrieve the User Code Deployment Namespace to be used for {@link ClassLoader} awareness
     * during operations related to the structure associated with this configuration.
     *
     * @return Namespace Name for use with the {@link com.hazelcast.internal.namespace.NamespaceService},
     *         or {@code null} if there is no Namespace to associate with.
     * @since 5.4
     */
    @Nullable
    default String getNamespace() {
        return DEFAULT_NAMESPACE;
    }
}
