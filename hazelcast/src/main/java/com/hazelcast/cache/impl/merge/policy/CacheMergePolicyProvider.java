/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;

/**
 * A provider for {@link com.hazelcast.cache.CacheMergePolicy} instances.
 */
public final class CacheMergePolicyProvider {

    private final ConcurrentMap<String, CacheMergePolicy> mergePolicyMap;
    private final NodeEngine nodeEngine;

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

    public CacheMergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.mergePolicyMap = new ConcurrentHashMap<String, CacheMergePolicy>();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        for (BuiltInCacheMergePolicies mergePolicy : BuiltInCacheMergePolicies.values()) {
            final CacheMergePolicy cacheMergePolicy = mergePolicy.newInstance();
            // Register `CacheMergePolicy` by its constant
            mergePolicyMap.put(mergePolicy.name(), cacheMergePolicy);
            // Register `CacheMergePolicy` by its name
            mergePolicyMap.put(mergePolicy.getImplementationClassName(), cacheMergePolicy);
        }
    }

    public CacheMergePolicy getMergePolicy(String className) {
        if (className == null) {
            throw new InvalidConfigurationException("Class name is mandatory!");
        }
        return ConcurrencyUtil.getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }

}
