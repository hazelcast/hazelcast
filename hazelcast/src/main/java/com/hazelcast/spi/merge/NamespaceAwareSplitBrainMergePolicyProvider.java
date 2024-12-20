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

package com.hazelcast.spi.merge;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nullable;

import static com.hazelcast.internal.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.internal.nio.ClassLoaderUtil.tryLoadClass;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * A provider for {@link SplitBrainMergePolicy} instances that supports namespace awareness.
 * <p>
 * Registers both out-of-the-box merge policies, identified by their fully qualified and simple class names,
 * and custom merge policies based on the user-defined namespace container name.
 *
 * @since 6.0
 */
public final class NamespaceAwareSplitBrainMergePolicyProvider extends SplitBrainMergePolicyProvider {

    private final NodeEngine nodeEngine;

    /**
     * Constructs a new provider for {@link SplitBrainMergePolicy} classes.
     *
     * @param nodeEngine the {@link NodeEngine} to retrieve the classloader from
     */
    public NamespaceAwareSplitBrainMergePolicyProvider(NodeEngine nodeEngine) {
        super(nodeEngine.getConfigClassLoader());
        this.nodeEngine = nodeEngine;
    }

    /**
     * Resolves the {@link SplitBrainMergePolicy} class by its classname within the specified namespace.
     * If the namespace is {@code null}, the default namespace is used.
     *
     * @param className the merge policy classname to resolve
     * @param namespace user namespace container name
     * @return the resolved {@link SplitBrainMergePolicy} class
     * @throws InvalidConfigurationException when the classname could not be resolved
     */
    @Override
    public SplitBrainMergePolicy getMergePolicy(String className, @Nullable String namespace) {
        if (className == null) {
            throw new InvalidConfigurationException("Class name is mandatory!");
        }
        // Predefined/out-of-the-box policies are added to the map during initialization
        // Predefined merge policies are accessible across all namespaces
        if (isPredefinedMergePolicy(className)) {
            return mergePolicyMap.get(className);
        }
        ClassLoader classLoader = NamespaceUtil.getClassLoaderForNamespace(nodeEngine, namespace);
        var mergePolicyClass = validateAndGetClass(className, classLoader);
        return getOrPutIfAbsent(mergePolicyMap, className, n -> createSplitBrainMergePolicy(mergePolicyClass, classLoader));
    }

    private Class<SplitBrainMergePolicy> validateAndGetClass(String className, ClassLoader classLoader) {
        try {
            return tryLoadClass(className, classLoader);
        } catch (ClassNotFoundException e) {
            throw new InvalidConfigurationException("Invalid SplitBrainMergePolicy: " + className, e);
        }
    }

    private SplitBrainMergePolicy createSplitBrainMergePolicy(Class clazz, ClassLoader loader) {
        try {
            return newInstance(loader, clazz);
        } catch (Exception e) {
            throw new InvalidConfigurationException("Invalid SplitBrainMergePolicy: " + clazz.getName(), e);
        }
    }
}
