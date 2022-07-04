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

package com.hazelcast.spi.merge;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * A provider for {@link SplitBrainMergePolicy} instances.
 * <p>
 * Registers out-of-the-box merge policies by their fully qualified and simple class name.
 *
 * @since 3.10
 */
public final class SplitBrainMergePolicyProvider {

    private static final Map<String, SplitBrainMergePolicy> OUT_OF_THE_BOX_MERGE_POLICIES;

    static {
        OUT_OF_THE_BOX_MERGE_POLICIES = new HashMap<>();
        addPolicy(DiscardMergePolicy.class, new DiscardMergePolicy());
        addPolicy(ExpirationTimeMergePolicy.class, new ExpirationTimeMergePolicy());
        addPolicy(HigherHitsMergePolicy.class, new HigherHitsMergePolicy());
        addPolicy(HyperLogLogMergePolicy.class, new HyperLogLogMergePolicy());
        addPolicy(LatestAccessMergePolicy.class, new LatestAccessMergePolicy());
        addPolicy(LatestUpdateMergePolicy.class, new LatestUpdateMergePolicy());
        addPolicy(PassThroughMergePolicy.class, new PassThroughMergePolicy());
        addPolicy(PutIfAbsentMergePolicy.class, new PutIfAbsentMergePolicy());
    }

    private final ClassLoader configClassLoader;

    private final ConcurrentMap<String, SplitBrainMergePolicy> mergePolicyMap
            = new ConcurrentHashMap<String, SplitBrainMergePolicy>();

    private final ConstructorFunction<String, SplitBrainMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, SplitBrainMergePolicy>() {
        @Override
        public SplitBrainMergePolicy createNew(String className) {
            try {
                return newInstance(configClassLoader, className);
            } catch (Exception e) {
                throw new InvalidConfigurationException("Invalid SplitBrainMergePolicy: " + className, e);
            }
        }
    };

    /**
     * Constructs a new provider for {@link SplitBrainMergePolicy} classes.
     *
     * @param nodeEngine the {@link NodeEngine} to retrieve the classloader from
     */
    public SplitBrainMergePolicyProvider(ClassLoader configClassLoader) {
        this.configClassLoader = configClassLoader;
        this.mergePolicyMap.putAll(OUT_OF_THE_BOX_MERGE_POLICIES);
    }

    /**
     * Resolves the {@link SplitBrainMergePolicy} class by its classname.
     *
     * @param className the merge policy classname to resolve
     * @return the resolved {@link SplitBrainMergePolicy} class
     * @throws InvalidConfigurationException when the classname could not be resolved
     */
    public SplitBrainMergePolicy getMergePolicy(String className) {
        if (className == null) {
            throw new InvalidConfigurationException("Class name is mandatory!");
        }
        return getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }

    private static <T extends SplitBrainMergePolicy> void addPolicy(Class<T> clazz, T policy) {
        OUT_OF_THE_BOX_MERGE_POLICIES.put(clazz.getName(), policy);
        OUT_OF_THE_BOX_MERGE_POLICIES.put(clazz.getSimpleName(), policy);
    }
}
