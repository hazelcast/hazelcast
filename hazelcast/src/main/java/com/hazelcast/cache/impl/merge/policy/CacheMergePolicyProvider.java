/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;

/**
 * A provider for {@link com.hazelcast.cache.impl.merge.policy.CacheMergePolicy} instances.
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
                throw ExceptionUtil.rethrow(e);
            }
        }
    };

    public CacheMergePolicyProvider(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        mergePolicyMap = new ConcurrentHashMap<String, CacheMergePolicy>();
        addOutOfBoxPolicies();
    }

    private void addOutOfBoxPolicies() {
        for (BuiltInCacheMergePolicies mergePolicy : BuiltInCacheMergePolicies.values()) {
            mergePolicyMap.put(mergePolicy.getName(), mergePolicy.newInstance());
        }
    }

    public CacheMergePolicy getMergePolicy(String className) {
        if (className == null) {
            throw new NullPointerException("Class name is mandatory!");
        }
        return ConcurrencyUtil.getOrPutIfAbsent(mergePolicyMap, className, policyConstructorFunction);
    }

}
