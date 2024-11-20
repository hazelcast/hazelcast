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

package com.hazelcast.internal.namespace.impl;

import com.hazelcast.function.ThrowingRunnable;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.LocalUserCodeNamespaceStats;
import com.hazelcast.internal.namespace.UserCodeNamespaceService;
import com.hazelcast.internal.namespace.ResourceDefinition;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * No-operations implementation for {@link UserCodeNamespaceService} used when Namespaces are disabled.
 * Replaced by {@code NamespaceServiceImpl} from Enterprise Edition when Namespaces are enabled.
 */
public final class NoOpUserCodeNamespaceService
        implements UserCodeNamespaceService {
    /**
     * {@link NodeEngine#getConfigClassLoader()} defined {@link ClassLoader} to use in invocations
     * where returning {@code null} would break functionality.
     */
    private final ClassLoader configClassLoader;

    public NoOpUserCodeNamespaceService(ClassLoader configClassLoader) {
        this.configClassLoader = configClassLoader;
    }

    @Override
    public void addNamespace(@Nonnull String nsName, @Nonnull Collection<ResourceDefinition> resources) {
        // No-op
    }

    @Override
    public boolean hasNamespace(String namespace) {
        return false;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public boolean isDefaultNamespaceDefined() {
        return false;
    }

    @Override
    public void setupNamespace(@Nullable String namespace) {
        // No-op
    }

    @Override
    public void cleanupNamespace(@Nullable String namespace) {
        // No-op
    }

    @Override
    public void runWithNamespace(@Nullable String namespace, ThrowingRunnable runnable) {
        runnable.run();
    }

    @Override
    public <V> V callWithNamespace(@Nullable String namespace, Callable<V> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

    @Override
    public ClassLoader getClassLoaderForNamespace(String namespace) {
        return configClassLoader;
    }

    @Override
    public Map<String, LocalUserCodeNamespaceStats> getStats() {
        return Collections.emptyMap();
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        // No-op
    }
}
