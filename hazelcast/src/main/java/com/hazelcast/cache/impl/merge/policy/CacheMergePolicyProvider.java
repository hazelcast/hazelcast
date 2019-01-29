/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.merge.policy;

import com.hazelcast.cache.BuiltInCacheMergePolicies;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * A provider for {@link com.hazelcast.cache.CacheMergePolicy} instances.
 */
public final class CacheMergePolicyProvider {

    private final ConcurrentMap<String, CacheMergePolicy> mergePolicyMap = new ConcurrentHashMap<String, CacheMergePolicy>();

    private final ConstructorFunction<String, CacheMergePolicy> policyConstructorFunction
            = new ConstructorFunction<String, CacheMergePolicy>() {
        @Override
        public CacheMergePolicy createNew(String className) {
            try {
                return newInstance(nodeEngine.getConfigClassLoader(), className);
            } catch (Exception e) {
                nodeEngine.getLogger(getClass()).severe(e);
                throw new InvalidConfigurationException("Invalid cache merge policy: " + className, e);
            }
        }
    };

    private final NodeEngine nodeEngine;
    private final SplitBrainMergePolicyProvider policyProvider;

    public CacheMergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.policyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        for (BuiltInCacheMergePolicies mergePolicy : BuiltInCacheMergePolicies.values()) {
            CacheMergePolicy cacheMergePolicy = mergePolicy.newInstance();
            // register `CacheMergePolicy` by its constant
            mergePolicyMap.put(mergePolicy.name(), cacheMergePolicy);
            // register `CacheMergePolicy` by its name
            mergePolicyMap.put(mergePolicy.getImplementationClassName(), cacheMergePolicy);
        }
    }

    /**
     * Returns an instance of a merge policy by its classname.
     * <p>
     * First tries to resolve the classname as {@link SplitBrainMergePolicy},
     * then as {@link com.hazelcast.cache.CacheMergePolicy}.
     * <p>
     * If no merge policy matches an {@link InvalidConfigurationException} is thrown.
     *
     * @param className the classname of the given merge policy
     * @return an instance of the merge policy class
     * @throws InvalidConfigurationException if no matching merge policy class was found
     */
    public Object getMergePolicy(String className) {
        if (className == null) {
            throw new InvalidConfigurationException("Class name is mandatory!");
        }
        try {
            return policyProvider.getMergePolicy(className);
        } catch (InvalidConfigurationException e) {
            return getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
        }
    }
}
